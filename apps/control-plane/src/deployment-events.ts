import { Cause, Clock, Context, Duration, Effect, HashMap, Layer, Option, PubSub, Ref, Schedule, Stream } from "effect"
import type { DeploymentState } from "@flux/contracts"
import { TemporalClient } from "./temporal-client.ts"

/**
 * Real-time deployment state for the control plane.
 *
 * A Temporal `Query` is pull-only and a workflow can't push events out of band
 *, so this service owns an internal `PubSub` fed by a poller: on a fixed
 * schedule it lists the running deployments, queries each one's state, and
 * publishes only the *deltas* (a state that is new or has changed). `watch`
 * gives a subscriber the current state immediately, then every subsequent
 * change — which is what `flux status --watch` streams over the socket.
 */
export interface DeploymentEvent {
  readonly workflowId: string
  readonly state: DeploymentState
  /**
   * When this state was read, stamped *after* the query returned. `watch` uses
   * it to drop a state older than one it has already emitted. Not a total order
   * over a distributed read — a query can return a stale value — but it removes
   * the interleaving that actually happens here, where a subscriber's own
   * snapshot and a poller tick read the same workflow moments apart.
   */
  readonly readAt: number
}

export class DeploymentEvents extends Context.Service<DeploymentEvents, {
  readonly watch: (workflowId: string) => Stream.Stream<DeploymentState>
}>()("DeploymentEvents") {}

export interface DeploymentEventsConfig {
  /** How often the poller samples deployment state. */
  readonly pollInterval: Duration.Input
  /** Upper bound on deployments tracked per tick (visibility page size). */
  readonly maxTracked: number
  /**
   * Called once with the service name when a tracked deployment leaves the
   * running set (it finished). Used to release its admission slot.
   * Defaults to a no-op so the poller stays decoupled from admission control.
   */
  readonly onDeploymentEnded?: (service: string) => Effect.Effect<void>
  /**
   * Called with the service name the first time a tick observes a deployment
   * running. Used to re-seat admission after a control-plane restart: the STM
   * map lives in memory, so a restart forgets every in-flight deployment and
   * the global budget silently becomes too permissive while they are still
   * running. Must be idempotent — the common case is a deployment this process
   * admitted seconds ago, which is already seated.
   * Defaults to a no-op so the poller stays decoupled from admission control.
   */
  readonly onDeploymentSeen?: (service: string) => Effect.Effect<void>
  /**
   * The admission slots to reconcile against, if any. Without this the poller
   * is purely edge-triggered and can only release what it watched run, which
   * leaks a slot for every deployment it never saw (see `reconcile` below).
   */
  readonly slots?: Effect.Effect<
    ReadonlyArray<{ readonly service: string; readonly owner: string | undefined; readonly seatedAt: number }>
  >
  /**
   * How long a slot with no owner yet may live. It covers the window between
   * the handler reserving a slot and the workflow it started being bound to it,
   * so it only has to outlast one `start` call.
   */
  readonly unownedGrace?: Duration.Input
}

/**
 * A per-subscription cursor that drops anything read before what it has already
 * let through. Exported because the stream it guards cannot exercise it: in the
 * poller's test harness a delta always carries a later stamp than the snapshot,
 * so a test at that level would pass whether or not the guard exists. The
 * interleaving it defends against needs two reads of the same workflow landing
 * out of order, which is a race, not a sequence a test can stage.
 */
export const keepMonotonic = (): (entry: { readonly readAt: number }) => boolean => {
  let latest = 0
  return (entry) => {
    if (entry.readAt < latest) return false
    latest = entry.readAt
    return true
  }
}

/** Two states are equal for delta purposes when their observable fields match. */
const sameState = (a: DeploymentState, b: DeploymentState): boolean =>
  a.phase === b.phase &&
  a.currentPercent === b.currentPercent &&
  a.stepIndex === b.stepIndex &&
  a.outcome === b.outcome

export const layer = (
  config: DeploymentEventsConfig
): Layer.Layer<DeploymentEvents, never, TemporalClient> =>
  Layer.effect(
    DeploymentEvents,
    Effect.gen(function*() {
      const temporal = yield* TemporalClient
      const pubsub = yield* PubSub.unbounded<DeploymentEvent>()
      const lastSeen = yield* Ref.make(HashMap.empty<string, DeploymentState>())

      const onEnded = config.onDeploymentEnded ?? (() => Effect.void)
      const onSeen = config.onDeploymentSeen ?? (() => Effect.void)

      /**
       * `status` for a single deployment, with anything that goes wrong turned
       * into `None` rather than propagated: one unreadable deployment must not
       * cost every other deployment in the tick its delta, which is what used
       * to happen.
       *
       * `catchCause` rather than `Effect.option`, and the difference is no
       * longer about the port lying. Its errors are typed now, so `option`
       * would be enough for anything Temporal can do. This still contains a
       * *defect*, because a bug in decoding one deployment taking down every
       * watcher is the worse outcome, but it logs one at error level rather
       * than swallowing it. A contained bug that nobody can see is how the
       * first version of this got written.
       */
      const statusOrNone = (workflowId: string) =>
        temporal.status(workflowId).pipe(
          Effect.map(Option.some),
          Effect.catchCause((cause) =>
            Effect.as(
              Cause.hasDies(cause)
                ? Effect.logError("bug reading a deployment's status", cause).pipe(
                  Effect.annotateLogs({ workflowId })
                )
                : Effect.void,
              Option.none<DeploymentState>()
            ))
        )

      // One poll: publish a delta for every running deployment whose state
      // changed (or is newly seen), then release the slot of any deployment that
      // was running and has now finished.
      const publishDeltas = Effect.gen(function*() {
        const runningIds = yield* temporal.listRunningIds(config.maxTracked)
        const runningSet = new Set(runningIds)
        const previous = yield* Ref.get(lastSeen)
        let next = HashMap.empty<string, DeploymentState>()

        for (const workflowId of runningIds) {
          const state = yield* statusOrNone(workflowId)
          if (Option.isNone(state)) continue

          const prior = HashMap.get(previous, workflowId)
          if (Option.isNone(prior)) {
            // First sighting: seat it, in case this process did not admit it.
            yield* onSeen(state.value.service)
          }
          const changed = Option.isNone(prior) || !sameState(prior.value, state.value)
          if (changed) {
            const readAt = yield* Clock.currentTimeMillis
            yield* PubSub.publish(pubsub, { workflowId, state: state.value, readAt })
          }
          next = HashMap.set(next, workflowId, state.value)
        }

        // Deployments we were tracking that are no longer running have ended.
        // A finished workflow leaves the running set before the poller sees its
        // terminal state, so query it once more and publish the final state (it
        // carries the `outcome`) — that's what lets `watch` close on its own —
        // then release its admission slot.
        for (const workflowId of HashMap.keys(previous)) {
          if (runningSet.has(workflowId)) continue
          const last = HashMap.get(previous, workflowId)
          if (Option.isSome(last)) {
            const final = yield* statusOrNone(workflowId)
            if (Option.isSome(final) && final.value.outcome !== undefined) {
              const readAt = yield* Clock.currentTimeMillis
              yield* PubSub.publish(pubsub, { workflowId, state: final.value, readAt })
            }
            yield* onEnded(last.value.service)
          }
        }

        yield* Ref.set(lastSeen, next)
      })

      /**
       * Deltas and reconciliation fail independently, and that separation is
       * the practical payoff of the port no longer hiding its errors behind
       * defects. Listing running deployments goes through Temporal's
       * *visibility index*, while reconciling asks `describe`, which reads
       * history directly. A degraded index is the common shape of a Temporal
       * outage, and it is exactly the case where reconciliation must keep
       * running: the deltas are only a stream going quiet for a few seconds,
       * whereas a slot not released is a service blocked. While the two shared
       * one `Effect.gen`, a visibility failure took the safety net down with
       * the thing it was there to catch.
       */
      const tick = Effect.gen(function*() {
        yield* publishDeltas.pipe(
          Effect.catchTag("TemporalUnavailable", (error) =>
            Effect.logWarning("visibility unavailable, no deployment deltas this tick").pipe(
              Effect.annotateLogs({ operation: error.operation, detail: error.detail })
            ))
        )
        yield* reconcile()
      })

      /**
       * The safety net under the edge-triggered release above, and the reason
       * it exists is a leak observed on a live cluster: a service stayed
       * rejected with `ServiceAlreadyDeploying` until the control plane
       * restarted. The loop before this one can only release what it has
       * *seen running*, so any deployment it never sees keeps its slot for the
       * life of the process. Two reproduced ways in: a deployment shorter than
       * one poll interval, and a deployment whose only sighting fell in a tick
       * that died.
       *
       * So rather than infer the end from a transition it might have missed,
       * this asks Temporal about the workflow that owns each slot. `execution`
       * is a `describe`, not a visibility query, because this answer frees a
       * slot and an eventually-consistent index would report a live rollout as
       * gone. `unknown` leaves the slot alone: a transient failure is not
       * evidence of anything, and the next tick will ask again.
       *
       * A slot with no owner is one whose workflow has not started yet, so
       * there is nothing to ask about; it is released only once it outlives the
       * grace period, which covers a `start` call that died between reserving
       * and binding.
       */
      const reconcile = () =>
        Effect.gen(function*() {
          const slots = config.slots
          if (slots === undefined) return
          const held = yield* slots
          if (held.length === 0) return
          const now = yield* Clock.currentTimeMillis
          const graceMs = Duration.toMillis(config.unownedGrace ?? "30 seconds")

          for (const slot of held) {
            if (slot.owner === undefined) {
              if (now - slot.seatedAt >= graceMs) {
                yield* Effect.logWarning("releasing an admission slot that was never bound to a workflow").pipe(
                  Effect.annotateLogs({ service: slot.service, ageMs: now - slot.seatedAt })
                )
                yield* onEnded(slot.service)
              }
              continue
            }
            const state = yield* temporal.execution(slot.owner)
            if (state === "open" || state === "unknown") continue
            yield* Effect.logInfo("releasing an admission slot whose workflow is gone").pipe(
              Effect.annotateLogs({ service: slot.service, workflowId: slot.owner, execution: state })
            )
            yield* onEnded(slot.service)
          }
        })

      // A failing tick (e.g. a transient visibility error) must not kill the
      // loop — log the cause and keep polling on the next schedule.
      yield* Effect.forkScoped(
        tick.pipe(
          Effect.catchCause((cause) => Effect.logWarning("deployment poll tick failed", cause)),
          Effect.repeat(Schedule.spaced(config.pollInterval))
        )
      )

      const watch = (workflowId: string): Stream.Stream<DeploymentState> =>
        Stream.unwrap(
          Effect.gen(function*() {
            // Subscribe first, then read the current state, so no delta emitted
            // between the two is lost.
            const subscription = yield* PubSub.subscribe(pubsub)
            const current = yield* statusOrNone(workflowId)
            const readAt = yield* Clock.currentTimeMillis

            const deltas = Stream.fromSubscription(subscription).pipe(
              Stream.filter((event) => event.workflowId === workflowId),
              Stream.map((event) => ({ state: event.state, readAt: event.readAt }))
            )

            const stamped = Option.match(current, {
              onNone: () => deltas,
              onSome: (state) => Stream.concat(Stream.make({ state, readAt }), deltas)
            })

            // Subscribing before reading the snapshot means a tick landing
            // between the two is both published and reflected in the snapshot,
            // and the two reads can land in either order — a subscriber could
            // otherwise be shown `monitoring @ 10%` and then `health-checking
            // @ 0%`. The cursor is created here, so it is local to one
            // subscription rather than shared across them.
            const stream = stamped.pipe(
              Stream.filter(keepMonotonic()),
              Stream.map((entry) => entry.state)
            )

            // Subscribing before reading the snapshot means a poll landing
            // between the two is published *and* included in the snapshot, so
            // the subscriber would see the same state twice. Losing an event is
            // worse than repeating one, so the order stays and the repeat is
            // collapsed here, against the same fields the poller compares.
            const deduped = Stream.changesWith(stream, sameState)

            // Emit the terminal state (the one carrying an `outcome`) and then
            // complete, so `flux status --watch` exits instead of hanging on a
            // finished deployment.
            return Stream.takeUntil(deduped, (state) => state.outcome !== undefined)
          })
        )

      return { watch }
    })
  )
