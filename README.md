> [!NOTE]
> A learning project — I built it to go deep on Effect v4 and Temporal, not to run in production.

# flux

A canary deployment tool. It moves traffic to a new version a step at a time,
watches error rate and latency, and rolls back if they get worse. The
orchestration is a Temporal workflow, so a crash or a long monitoring window
doesn't lose it. It drives nginx or Caddy and reads Prometheus. No Kubernetes.

[![Effect](https://img.shields.io/badge/Effect-4.0--beta-ff5faa.svg)](https://effect.website/)
[![Temporal](https://img.shields.io/badge/Temporal-1.20-000000.svg)](https://temporal.io/)

![A canary promoting itself 10% → 50% → 100%](docs/demo.gif)

## How a deployment runs

You give it a service, the new version, and the one to fall back to. The workflow
health-checks the new version, shifts 10% of traffic, watches the metrics for a
while, then 50%, then 100%. If a metric crosses its budget it rolls back. A step
can pause for a manual approval.

The constraint I cared about is the split between Temporal and Effect:

- The **workflow** is plain deterministic TypeScript. No Effect, no I/O, no
  `Date.now()`. Temporal replays it, so it has to stay pure.
- The **activities** are where Effect runs, on one runtime per worker. They probe
  the URL, rewrite the nginx config, query Prometheus. Each one validates its
  input with a Schema and turns typed failures into Temporal failures so the
  reason survives the wire.

Rollback is a saga. The first traffic shift registers a compensation that puts
the previous version back at 100%, and any ending that isn't a success runs it.

```
  flux deploy --service api --version v2
        │
        ▼
  ┌─────────────────────────────────┐            ┌───────────────────────────┐
  │ Temporal   (durable, no Effect) │            │ worker   (Effect runtime) │
  │ deploymentWorkflow              │            │                           │
  │                                 │            │ activities:               │
  │  1. health check                │  ───────►  │   health   (local)        │
  │  2. shift 10%                   │            │   router   (nginx reload) │
  │  3. monitor (heartbeat)         │            │   metrics  (Prometheus)   │
  │  4. breach?  -> roll back       │            │   notify   (Slack)        │
  │  5. approve gate (update)       │            └───────────────────────────┘
  │  6. -> Succeeded                │
  └─────────────────────────────────┘
```

## Some things I wanted to try

Choices that go past plumbing:

- Two threshold rules that share a PromQL query hit Prometheus once per poll, not
  twice, through a `RequestResolver`.
- Admission control (one deployment per service, plus a global cap) is a single
  STM transaction: a `TxSemaphore` and a `TxHashMap` updated together, so two
  concurrent triggers can't over-admit.
- `status --watch` streams live state over a websocket, fed by a `PubSub` a poller
  writes to.
- `/stats` answers questions Temporal's visibility can't — rollback rate per
  service, mean canary duration — from a small SQLite read model.
- A multi-service rollout is a parent workflow over one child per service, with a
  fail-fast policy that aborts the siblings if one goes bad.
- An abort cancels the in-flight monitor immediately (a `CancellationScope`)
  instead of waiting out the window.
- The router port has two deliberately opposite implementations — nginx renders
  a file and reloads a process behind a lock; Caddy PATCHes its admin API,
  stateless and lock-free — and the same canary passes through both without a
  line changing above the port.
- Payloads above 1 KiB are gzipped on the wire and in Temporal's history by a
  codec that never enters the workflow VM; a `/codec` endpoint lets the
  Temporal UI read them back. The integration test asserts the stored history
  payload really is `binary/gzip`.
- A deployment is one trace, not two. Client and activity interceptors carry a
  W3C `traceparent` through Temporal's own headers. The workflow has to forward
  it to every activity it schedules itself; Temporal doesn't do that for you.
  This replaced an earlier version that faked a trace root from the workflow's
  run id. Proven the same way as the codec: the raw history shows the same
  header on the start event and the first activity's scheduled event.
- A separate experiment reimplements the same canary (same domain types, same
  activities) on Effect's own `effect/unstable/workflow` instead of Temporal.
  It lives in `packages/comparison`, is never imported by the running app, and
  its durability is proven the blunt way: a test SIGKILLs the process
  mid-monitor and a fresh process resumes the canary from the same SQLite file,
  replaying completed steps instead of redoing them. The full write-up of what
  each engine buys is in [docs/comparison.md](docs/comparison.md).
- A deploy can be fenced to a time window: `flux deploy --window "* 9-17 * * 1-5"`
  refuses to start outside weekday business hours and tells you when the window
  next opens. The window is a cron expression evaluated by a pure domain
  function before admission; it never reaches the workflow. (Building it caught
  that Effect's `Cron` is second-precise, so a range window needs the clock
  floored to the minute.)
- The workflow's code has actually evolved once, the way you would in
  production: a new `started` notification added behind `workflow.patched()`.
  The committed replay histories prove both directions: old histories replay
  the old path, and the same edit without the patch guard fails the replay
  test with a determinism error. The lock also refuses `deprecatePatch` while
  those histories exist, which is the patch lifecycle doing its job.

## Layout

A pnpm + Turborepo monorepo.

| Package | Role |
|---|---|
| `@flux/domain` | Schemas, tagged errors, pure rules |
| `@flux/application` | Use cases and the four ports: metrics, router, health, notify |
| `@flux/adapters` | Port implementations: Prometheus, nginx, Caddy, HTTP health, Slack |
| `@flux/orchestration` | Temporal workflows and activities |
| `@flux/comparison` | The same canary on Effect's own workflow engine — an experiment, not shipped |
| `@flux/contracts` | Shared HTTP + RPC schemas, so the CLI and control plane agree |
| `@flux/config` | TOML + env configuration |
| `apps/worker` | Runs the Temporal worker |
| `apps/control-plane` | HTTP API, websocket watch, SQLite read model |
| `apps/cli` | `flux` — deploy, deploy-multi, drift, status, stats, approve, abort, history |

## Running it

Node ≥ 22, pnpm 11. The backing services run in Docker:

```bash
docker compose up -d postgresql temporal prometheus jaeger temporal-ui
pnpm install
pnpm typecheck && pnpm test
```

Then, in separate terminals:

```bash
pnpm --filter @flux/worker dev                 # the Temporal worker
pnpm --filter @flux/control-plane dev          # HTTP API on :8080, OpenAPI at /docs
pnpm --filter @flux/cli dev -- deploy --service api --version v2 --previous-version v1
pnpm --filter @flux/cli dev -- status --workflow-id <id> --watch
```

Temporal UI is at :8233, Jaeger at :16686, Prometheus at :9090. Config lives in
`flux.config.toml`; environment variables override it.

## What's proven

Three tiers, because a learning project is only worth as much as what holds up.

**End to end, against a real (time-skipping) Temporal.** The canary workflow
(sequencing, saga rollback, approval gate, typed-failure → `Failed`). The control
plane's client, trigger through outcome. Multi-service fail-fast. The cancellable
monitor. `continueAsNew`. And, with the real adapters pointed at local HTTP
doubles, a full canary to `Succeeded` that checks the side effects actually
happened: the health endpoint got probed, the nginx config got written.
Two captured histories — a promotion and a rollback — are committed and
replayed against the current workflow code on every run, so an edit that would
break in-flight deployments fails as a determinism error before it ships.

**In isolation.** The STM admission controller (five concurrent triggers, a
budget of two, exactly two admitted). Drift comparison and reconciliation. The
SQLite projection and its aggregation query. The poller's delta suppression.

**Against a real cluster, in CI.** The time-skipping server implements neither
worker versioning, nor the tuner's native config, nor Schedules, nor Nexus, so
a second CI job boots the repo's own compose and proves them for real. A
versioned worker pins the workflow it ran (the `describe` shows the deployment
and build id). A worker running the production tuner completes a canary. The
drift Schedule's create → update-in-place → delete lifecycle holds. A second
namespace triggers a canary in a separate platform namespace through a
registered Nexus endpoint, with no other access to it, and the run completes
under the workflow id the cross-namespace call actually produced. And a worker
SIGKILLed mid-monitor loses nothing: the server notices the missed heartbeats
and a fresh worker finishes the canary, replaying the completed steps instead
of redoing them. This tier exists because running things for real kept finding
bugs the type checker was happy with.

---

*Effect is pinned to an exact beta (`4.0.0-beta.97`); upgrades are deliberate.
MIT licensed.*
