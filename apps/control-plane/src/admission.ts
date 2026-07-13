import { Context, Effect, Layer, TxHashMap, TxSemaphore } from "effect"
import { DeploymentBudgetExhausted, ServiceAlreadyDeploying } from "@flux/contracts"

/**
 * Admission control for deployments (N4/D14) — the genuine home of Effect STM.
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
 * A slot is released when the deployment ends — the poller (D11) observes the
 * terminal transition and calls `release` — or immediately if the workflow fails
 * to start.
 */
export class AdmissionController extends Context.Service<AdmissionController, {
  /** Reserve a slot for `service`, or reject if the budget is full / it is already deploying. */
  readonly admit: (service: string) => Effect.Effect<void, DeploymentBudgetExhausted | ServiceAlreadyDeploying>
  /** Free the service's slot (idempotent). */
  readonly release: (service: string) => Effect.Effect<void>
  /** The services currently holding a slot. */
  readonly inFlight: Effect.Effect<ReadonlyArray<string>>
}>()("AdmissionController") {}

export const layer = (maxConcurrent: number): Layer.Layer<AdmissionController> =>
  Layer.effect(
    AdmissionController,
    Effect.gen(function*() {
      const budget = yield* TxSemaphore.make(maxConcurrent)
      const inflight = yield* TxHashMap.empty<string, true>()

      const admit = (service: string) =>
        Effect.tx(
          Effect.gen(function*() {
            if (yield* TxHashMap.has(inflight, service)) {
              return yield* Effect.fail(new ServiceAlreadyDeploying({ service }))
            }
            if (!(yield* TxSemaphore.tryAcquire(budget))) {
              return yield* Effect.fail(new DeploymentBudgetExhausted({ service, limit: maxConcurrent }))
            }
            yield* TxHashMap.set(inflight, service, true)
          })
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

      return { admit, release, inFlight: TxHashMap.keys(inflight) }
    })
  )
