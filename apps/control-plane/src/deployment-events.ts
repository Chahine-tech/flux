import { Context, type Duration, Effect, HashMap, Layer, Option, PubSub, Ref, Schedule, Stream } from "effect"
import type { DeploymentState } from "@flux/contracts"
import { TemporalClient } from "./temporal-client.ts"

/**
 * Real-time deployment state for the control plane (N3, D11).
 *
 * A Temporal `Query` is pull-only and a workflow can't push events out of band
 * (D6), so this service owns an internal `PubSub` fed by a poller: on a fixed
 * schedule it lists the running deployments, queries each one's state, and
 * publishes only the *deltas* (a state that is new or has changed). `watch`
 * gives a subscriber the current state immediately, then every subsequent
 * change — which is what `flux status --watch` streams over the socket (N3.4).
 */
export interface DeploymentEvent {
  readonly workflowId: string
  readonly state: DeploymentState
}

export class DeploymentEvents extends Context.Service<DeploymentEvents, {
  readonly watch: (workflowId: string) => Stream.Stream<DeploymentState>
}>()("DeploymentEvents") {}

export interface DeploymentEventsConfig {
  /** How often the poller samples deployment state. */
  readonly pollInterval: Duration.Input
  /** Upper bound on deployments tracked per tick (visibility page size). */
  readonly maxTracked: number
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

      // One poll: publish a delta for every running deployment whose state
      // changed (or is newly seen), then forget deployments that stopped running.
      const tick = Effect.gen(function*() {
        const runningIds = yield* temporal.listRunningIds(config.maxTracked)
        const previous = yield* Ref.get(lastSeen)
        let next = HashMap.empty<string, DeploymentState>()

        for (const workflowId of runningIds) {
          const state = yield* Effect.option(temporal.status(workflowId))
          if (Option.isNone(state)) continue

          const prior = HashMap.get(previous, workflowId)
          const changed = Option.isNone(prior) || !sameState(prior.value, state.value)
          if (changed) {
            yield* PubSub.publish(pubsub, { workflowId, state: state.value })
          }
          next = HashMap.set(next, workflowId, state.value)
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

            const deltas = Stream.fromSubscription(subscription).pipe(
              Stream.filter((event) => event.workflowId === workflowId),
              Stream.map((event) => event.state)
            )

            return Option.match(current, {
              onNone: () => deltas,
              onSome: (state) => Stream.concat(Stream.make(state), deltas)
            })
          })
        )

      return { watch }
    })
  )
