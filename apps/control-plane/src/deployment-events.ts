import { Clock, Context, type Duration, Effect, HashMap, Layer, Option, PubSub, Ref, Schedule, Stream } from "effect"
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

      // One poll: publish a delta for every running deployment whose state
      // changed (or is newly seen), then release the slot of any deployment that
      // was running and has now finished.
      const tick = Effect.gen(function*() {
        const runningIds = yield* temporal.listRunningIds(config.maxTracked)
        const runningSet = new Set(runningIds)
        const previous = yield* Ref.get(lastSeen)
        let next = HashMap.empty<string, DeploymentState>()

        for (const workflowId of runningIds) {
          const state = yield* Effect.option(temporal.status(workflowId))
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
            const final = yield* Effect.option(temporal.status(workflowId))
            if (Option.isSome(final) && final.value.outcome !== undefined) {
              const readAt = yield* Clock.currentTimeMillis
              yield* PubSub.publish(pubsub, { workflowId, state: final.value, readAt })
            }
            yield* onEnded(last.value.service)
          }
        }

        yield* Ref.set(lastSeen, next)
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
            const current = yield* Effect.option(temporal.status(workflowId))
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
