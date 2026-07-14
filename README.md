# flux

A canary deployment tool. It moves traffic to a new version a step at a time,
watches error rate and latency, and rolls back if they get worse. The
orchestration is a Temporal workflow, so a crash or a long monitoring window
doesn't lose it. It drives nginx and reads Prometheus. No Kubernetes.

I built it to go deep on Effect v4 and Temporal. It's a learning project, not
something I run in production.

[![Effect](https://img.shields.io/badge/Effect-4.0--beta-ff5faa.svg)](https://effect.website/)
[![Temporal](https://img.shields.io/badge/Temporal-1.20-000000.svg)](https://temporal.io/)

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

## Layout

A pnpm + Turborepo monorepo.

| Package | Role |
|---|---|
| `@flux/domain` | Schemas, tagged errors, pure rules |
| `@flux/application` | Use cases and the four ports: metrics, router, health, notify |
| `@flux/adapters` | Port implementations: Prometheus, nginx, HTTP health, Slack |
| `@flux/orchestration` | Temporal workflows and activities |
| `@flux/contracts` | Shared HTTP + RPC schemas, so the CLI and control plane agree |
| `@flux/config` | TOML + env configuration |
| `apps/worker` | Runs the Temporal worker |
| `apps/control-plane` | HTTP API, websocket watch, SQLite read model |
| `apps/cli` | `flux` — deploy, deploy-multi, drift, status, approve, abort, history |

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

**In isolation.** The STM admission controller (five concurrent triggers, a
budget of two, exactly two admitted). Drift comparison and reconciliation. The
SQLite projection and its aggregation query. The poller's delta suppression.

**Wired and type-checked, not yet run under test.** Worker deployment versioning,
the resource tuner, and creating the drift Schedule. The time-skipping test
server has no advanced visibility, versioning, or schedules, so those want a real
cluster.

---

*Effect is pinned to an exact beta (`4.0.0-beta.97`); upgrades are deliberate.
MIT licensed.*
