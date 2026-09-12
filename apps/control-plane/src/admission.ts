import { Clock, Context, Effect, Layer, Option, TxHashMap, TxSemaphore } from "effect"
import { DeploymentBudgetExhausted, ServiceAlreadyDeploying } from "@flux/contracts"

/**
 * Admission control for deployments — the genuine home of Effect STM.
 *
 * Two rules are enforced globally across the control plane: at most
 * `maxConcurrent` deployments run at once (blast-radius budget), and a service
 * has at most one deployment in flight (no self-collision). Both are checked and
 * reserved **atomically** so concurrent trigger requests can't over-admit: a
 * `TxSemaphore` (the budget) and a `TxHashMap` (the in-flight set) are mutated in
 * one `Effect.tx`, and v4's optimistic-retry transactions serialize the racers.
 * The two cells must stay consistent (a held permit ⇔ a set entry), which is
 * exactly the multi-cell invariant STM exists to protect.
 *
 * A slot is released when the deployment ends: the poller observes the terminal
 * transition and calls `release`, or it is freed immediately if the workflow
 * fails to start.
 *
 * That edge-triggered release is not sufficient on its own, and a live run
 * proved it. The poller can only release what it has *seen running*, so any
 * deployment it never sees keeps its slot forever and the service is rejected
 * with `ServiceAlreadyDeploying` until the process restarts. Two ways in, both
 * reproduced as tests: a deployment shorter than one poll interval, and a
 * deployment whose only sighting lands in a tick that died.
 *
 * So a slot also records **who owns it**, and the poller reconciles: a slot
 * whose workflow Temporal reports as closed is released whether or not the
 * poller ever watched it run. `bind` is what attaches the owner, called after
 * the workflow has actually started, since that is when its id exists. Between
 * `admit` and `bind` a slot is unowned, and an unowned slot is only released
 * once it is older than a grace period, because there is nothing yet to ask
 * Temporal about.
 *
 * The owner is the workflow that *justifies* the slot, not necessarily one
 * deploying that service: a multi-service rollout binds every one of its
 * services to the parent id. Its children start in waves, so a service waiting
 * for its wave has no running deployment of its own for minutes, and
 * reconciling against running deployments alone would free the slots
 * `admitAll` took precisely to reserve them.
 */
export class AdmissionController extends Context.Service<AdmissionController, {
  /** Reserve a slot for `service`, or reject if the budget is full / it is already deploying. */
  readonly admit: (service: string) => Effect.Effect<void, DeploymentBudgetExhausted | ServiceAlreadyDeploying>
  /**
   * Reserve a slot for every service of a multi-service rollout, all or nothing.
   * A rollout that cannot seat all of its services seats none of them, rather
   * than starting half a rollout and discovering the budget mid-flight.
   */
  readonly admitAll: (
    services: ReadonlyArray<string>
  ) => Effect.Effect<void, DeploymentBudgetExhausted | ServiceAlreadyDeploying>
  /**
   * Record the workflow that owns these services' slots, once it has started.
   * Ignores a service that holds no slot, so a late call cannot invent one.
   */
  readonly bind: (services: ReadonlyArray<string>, workflowId: string) => Effect.Effect<void>
  /** Free the service's slot (idempotent). */
  readonly release: (service: string) => Effect.Effect<void>
  /** Free every listed service's slot (idempotent). */
  readonly releaseAll: (services: ReadonlyArray<string>) => Effect.Effect<void>
  /** The services currently holding a slot. */
  readonly inFlight: Effect.Effect<ReadonlyArray<string>>
  /** Every slot with its owner and age, for the poller to reconcile against. */
  readonly slots: Effect.Effect<ReadonlyArray<Slot>>
}>()("AdmissionController") {}

/** One held slot. `owner` is unset between `admit` and `bind`. */
export interface Slot {
  readonly service: string
  readonly owner: string | undefined
  readonly seatedAt: number
}

export const layer = (maxConcurrent: number): Layer.Layer<AdmissionController> =>
  Layer.effect(
    AdmissionController,
    Effect.gen(function*() {
      const budget = yield* TxSemaphore.make(maxConcurrent)
      const inflight = yield* TxHashMap.empty<string, Omit<Slot, "service">>()

      /** Seat one service. Runs inside the caller's transaction, never its own. */
      const seat = (service: string, seatedAt: number) =>
        Effect.gen(function*() {
          if (yield* TxHashMap.has(inflight, service)) {
            return yield* Effect.fail(new ServiceAlreadyDeploying({ service }))
          }
          if (!(yield* TxSemaphore.tryAcquire(budget))) {
            return yield* Effect.fail(new DeploymentBudgetExhausted({ service, limit: maxConcurrent }))
          }
          yield* TxHashMap.set(inflight, service, { owner: undefined, seatedAt })
        })

      const admit = (service: string) =>
        Effect.flatMap(Clock.currentTimeMillis, (now) => Effect.tx(seat(service, now)))

      /**
       * All of them in one transaction. A failure anywhere rolls back the seats
       * already taken in this attempt, which is the property that makes this
       * worth STM rather than a loop over `admit`: seating four services out of
       * six and then failing would leave four permits held for a rollout that
       * never starts.
       */
      const admitAll = (services: ReadonlyArray<string>) =>
        Effect.flatMap(
          Clock.currentTimeMillis,
          (now) => Effect.tx(Effect.forEach(services, (service) => seat(service, now), { discard: true }))
        )

      const release = (service: string) =>
        Effect.tx(
          Effect.gen(function*() {
            if (yield* TxHashMap.has(inflight, service)) {
              yield* TxHashMap.remove(inflight, service)
              yield* TxSemaphore.release(budget)
            }
          })
        )

      const releaseAll = (services: ReadonlyArray<string>) =>
        Effect.forEach(services, release, { discard: true })

      /**
       * One transaction for the whole rollout: binding service by service could
       * leave half a rollout owned if it were interleaved with a release.
       */
      const bind = (services: ReadonlyArray<string>, workflowId: string) =>
        Effect.tx(
          Effect.forEach(services, (service) =>
            Effect.gen(function*() {
              const held = yield* TxHashMap.get(inflight, service)
              if (Option.isNone(held)) return
              yield* TxHashMap.set(inflight, service, { ...held.value, owner: workflowId })
            }), { discard: true })
        )

      const slots = Effect.map(
        TxHashMap.entries(inflight),
        (entries) => entries.map(([service, held]): Slot => ({ service, ...held }))
      )

      return { admit, admitAll, bind, release, releaseAll, inFlight: TxHashMap.keys(inflight), slots }
    })
  )
