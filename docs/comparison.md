# The same canary, twice: Temporal vs `effect/unstable/workflow`

flux's deployment workflow runs on Temporal. Effect v4 ships its own durable
workflow engine — `effect/unstable/workflow` over `effect/unstable/cluster` —
which is, on paper, a conceptual competitor. This document is the comparison I
promised myself at the start of the project: the **same canary** (health check,
staged traffic shifts, metric monitoring, saga rollback, manual approval gate)
implemented on both engines, run on both engines, compared on what actually
happened rather than on what documentation claims.

Ground rules. The Effect implementation (`packages/comparison`) reuses the
Temporal implementation's domain schemas and application use cases **unchanged**:
same `CanaryStep`, same `MetricRule`, same `healthCheck`/`shiftTraffic`/
`monitorStep` programs against the same ports. Only the orchestration layer
differs. Nothing below is asserted from docs alone; every claim traces to a
test in this repo. One caveat applies throughout: the Effect modules are
**undocumented and unstable** (nothing on the website; the only sources are the
shipped source and the repo's own tests), so this compares Temporal as it is
against Effect's engine as it is *today*.

## Where the code lives

| | Temporal | Effect |
|---|---|---|
| Workflow | `packages/orchestration/src/workflows/deployment.workflow.ts` | `packages/comparison/src/workflow.ts` |
| Activities | `packages/orchestration/src/activities/` | `packages/comparison/src/activities.ts` |
| Proof | time-skipping tests + real-cluster CI + replay lock | `packages/comparison/test/` incl. a SIGKILL/resume proof |

## The boundary is the biggest difference

Temporal imposes a hard wall: the workflow runs in a deterministic V8 isolate
that must never see the Effect runtime (this repo's rule D6), and activities
bridge into Effect through a `ManagedRuntime` and a Promise boundary. Keeping
that wall intact is a discipline the whole repo is organized around: separate
entry points, `import type` only, a bundle-purity check, a replay test to catch
violations.

On the Effect engine, **the wall does not exist**. An `Activity.make` body is an
ordinary `Effect`; the ports (`HealthPort`, `RouterPort`, …) are satisfied by
whatever `Layer` wraps the workflow, in the same runtime, same process. No
bridge, no serialization boundary inside the process, no second entry point.
The whole D6/D7 apparatus — the most carefully engineered part of the Temporal
side — is simply not needed.

That cuts both ways. The wall is also what makes Temporal's model *legible*:
you always know which side of it you are standing on. On the Effect side the
workflow body looks like any other Effect code while secretly being subject to
replay semantics; nothing in the type system distinguishes replay-safe from
replay-unsafe code. Which leads to:

## Determinism: same contract, different visibility

Both engines re-execute the workflow body from the top and replay completed
steps from storage. Resolving the approval gate on the Effect engine does not
resume "at the await point" — the handler function re-runs from its first line
and completed `Activity`s replay their cached exits. This is exactly Temporal's
replay model, just without a VM to make it obvious.

The difference that bites in practice: **what identifies a completed step**.
Temporal identifies activities by their *position in the recorded event
history*; Effect identifies them by *name* (`executionId/name/attempt`).
Calling `shiftTraffic` once per canary step with the same literal name made
every step after the first replay the first step's cached result: the canary
"succeeded" while shifting traffic exactly once. Found the hard way; fixed by
qualifying every activity name with the step index. Temporal has no equivalent
hazard, and nothing on the Effect side warns you.

## Durability: both proven, differently stored

Temporal persists an **event history** per execution; this repo proves the
model three ways: a real canary against a real cluster in CI, committed
histories replayed as a determinism lock, and (symmetric with the Effect-side
proof below) a worker `SIGKILL`ed mid-monitor (`apps/worker/test/
worker-kill.test.ts`): the server notices the missed heartbeats, reschedules
the in-flight activity onto a fresh worker, the completed traffic shift
replays from history without re-executing, and the canary completes.

The Effect engine persists **messages and replies** in SQL
(`ClusterWorkflowEngine` + `SqlMessageStorage`). Proven here the blunt way
(`test/persistence.test.ts`): run the canary over a SQLite file, `SIGKILL` the
process mid-monitoring-window, start a fresh process on the same file with the
same payload. The idempotency key derives the same execution id, the engine
redelivers the unprocessed execution, completed activities replay from SQL
without re-executing (the traffic-shift side effect appears exactly once in the
log), the in-flight monitor re-runs, and the canary completes. Kill -9, not a
graceful shutdown.

Proving it surfaced the starkest operational difference of the whole
comparison: **how long recovery takes**. A delivered-but-unreplied message only
becomes redeliverable once its `last_read` timestamp is older than a redelivery
lease **hardcoded at 10 minutes** in `SqlMessageStorage`. A fresh runner does
pick up a dead runner's execution, but by default up to ten minutes later.
There is a faster path (a duplicate `execute` racing shard acquisition can
revive it immediately), but it is a race: this very test flaked one run in
three on Linux until it stopped relying on it and aged `last_read` directly,
compressing wall-clock time the way Temporal's time-skipping server does on
the other side of this comparison. Temporal's equivalent knob is the activity
heartbeat timeout. This repo sets 30 seconds on the monitor, and the
worker-kill proof observed the whole kill → reschedule → complete cycle at
**~18 seconds** wall clock, tunable per activity. Eighteen seconds versus a
hardcoded ten minutes, both measured in this repo: for a canary whose job is
reacting to bad metrics within seconds, that gap is disqualifying on its own.

So durability holds on both sides. The operational difference is what you get
*around* it: Temporal's history is a first-class artifact — fetchable,
serializable to JSON, replayable in CI against future code (this repo commits
two of them as a determinism lock), inspectable in a UI. The Effect engine's
SQL rows are an implementation detail with no tooling around them. There is no
equivalent of the replay lock — nothing to catch a code change that breaks
in-flight executions.

## Errors and compensation: the typed channel changes the design

This is where the Effect engine is genuinely more interesting, not just
smaller.

Temporal workflows return one success-shaped value, so flux folds every outcome
— `Succeeded`, `RolledBack`, `Aborted` — into a result union, and rollback is a
hand-rolled saga: an array of compensations executed from a `catch` block. The
reason a rollback happened and the mechanism that performs it are connected by
convention.

The Effect engine gives workflows a **typed error channel**, and
`Workflow.withCompensation` fires its finalizers only when the workflow
*fails*. That forces an honest modeling decision: a breach genuinely has to be
a failure (`RolledBack` moved into the error schema) for the built-in saga to
engage at all. Two consequences, one in each direction:

- **For**: the compiler audits the failure surface. Declaring the workflow's
  error schema without `MetricsUnavailable`/`RouterUnavailable` did not
  typecheck — the compiler had noticed the reused activities could fail with
  them. Temporal has no equivalent static check; an undeclared
  `ActivityFailure` surfaces at runtime.
- **Against**: a compensation's own failure has nowhere typed to go. Its
  signature is `Effect<void, never, R>`, so a failed rollback becomes a defect.
  Temporal-side flux models `RollbackFailed` as a first-class, page-someone
  outcome. The same failure mode is structurally worse off on the Effect side.

Granularity also differs: Temporal-side flux registers the rollback
compensation once, at the first traffic divergence; `withCompensation` wraps
each effect, so per-step registration is the natural shape and a breach at step
N runs N (idempotent, redundant) compensations.

## Signals, updates, queries

The approval gate maps cleanly: Temporal's validated `approve` update ↔
Effect's `DurableDeferred` resolved by an external `done()` call with a token.
Comparable expressiveness, and the deferred's token scheme (workflow name +
execution id + deferred name) is arguably cleaner than signal names.

**Queries have no Effect equivalent.** Temporal-side flux exposes live canary
state (`flux status --watch` ultimately reads a workflow query). The Effect
engine offers `poll` — terminal result or "still running" — with no way to
observe intermediate state from outside. You would build that yourself.

## Testing

Temporal's time-skipping test server is the best thing about its DX: a 15-minute
canary tests in ~2 seconds, and the clock skips across the whole
server-workflow-activity system coherently.

The Effect engine tests in-process with `TestClock` — lighter (no binary
download, no server boot) but with two rough edges found empirically. A
`Stream.schedule` inside an `Activity` inside the engine's own fork did not
respond to one large `TestClock.adjust`; only a loop of small increments
interleaved with `yieldNow` drove it reliably. And `layerMemory` builds fresh
state per `Effect.provide`: providing the layer separately to `execute` and to
the external `DurableDeferred.done` silently constructs two disconnected
engines where the approval resolves in one and the workflow waits forever in
the other. Temporal's client/worker/server split makes that mistake
unrepresentable.

## What each side simply does not have

Temporal, missing from the Effect engine: a UI, history as an artifact, replay
testing, advanced visibility/search, worker versioning for in-flight
executions, payload codecs, schedules, Nexus-style cross-boundary calls, an
ecosystem of operational practice. Also: documentation.

Effect, missing from Temporal: activities as plain effects in the same runtime
(no bridge, no D6/D7 discipline), the typed error channel with
compiler-audited failure surfaces, compensation integrated with the error
model, schema-first payloads end to end without a converter layer, and a
dependency graph that is just `Layer`s all the way down.

## Verdict

For a deployment orchestrator that people operate — the thing flux pretends to
be — Temporal wins without drama. The determinism wall is a real tax, but what
it buys (history, replay locks, versioning, a UI, a test server, years of
operational practice) is exactly what running long-lived workflows in
production demands. The Effect engine's answer to "what happens to in-flight
canaries when I deploy new workflow code" is currently *nothing*, and that
question is the heart of this domain.

But the Effect engine is not a toy. The programming model is *better* in the
places Effect is always better: errors, composition, dependency injection,
one runtime instead of a bridge. If it grows the operational layer — history
as an artifact, replay tooling, versioning, docs — the wall-free model would be
a serious reason to choose it. Today it reads like what it is: the engine
Temporal's programming model deserves, several operational layers short of the
engine Temporal actually is.
