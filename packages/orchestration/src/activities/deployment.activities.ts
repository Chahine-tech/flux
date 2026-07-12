import { Duration, Effect, type ManagedRuntime } from "effect"
import type { HealthPort, MetricsPort, NotifyPort, RouterPort } from "@flux/application"
import { healthCheck, monitorStep, notify, shiftTraffic } from "@flux/application"
import { recordOutcome, recordTrafficShift } from "../metrics.ts"
import { type FluxError, toApplicationFailure } from "./activity-error.ts"
import type { DeploymentActivities } from "./types.ts"

/** Services the worker's ManagedRuntime must provide (the 4 ports). */
export type AppServices = HealthPort | MetricsPort | NotifyPort | RouterPort

/**
 * Bridge Effect → Promise for Temporal.
 *
 * Activities are built once at worker startup around a single `ManagedRuntime`
 * (holding the composed adapter Layers), instead of re-providing Layers per
 * call. Before running, each Effect's typed error is mapped to a Temporal
 * `ApplicationFailure` with the right retryable semantics (see activity-error.ts),
 * so the typed error survives the boundary instead of becoming an anonymous reject.
 */
export const createActivities = (
  runtime: ManagedRuntime.ManagedRuntime<AppServices, never>
): DeploymentActivities => {
  const run = <A, E extends FluxError>(
    effect: Effect.Effect<A, E, AppServices>
  ): Promise<A> => runtime.runPromise(Effect.mapError(effect, toApplicationFailure))

  return {
    healthCheck: (params) => run(healthCheck(params)),

    setTrafficWeight: (params) => run(shiftTraffic(params).pipe(Effect.tap(() => recordTrafficShift))),

    monitorStep: (params) =>
      run(
        monitorStep({
          service: params.service,
          version: params.version,
          window: Duration.millis(params.windowMs),
          pollInterval: Duration.millis(params.pollIntervalMs),
          rules: params.rules
        })
      ),

    notify: (params) => run(notify(params)),

    // Metric updates cannot fail (in-memory), so they skip the error mapping.
    recordOutcome: (outcome) => runtime.runPromise(recordOutcome(outcome))
  }
}
