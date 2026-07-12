import { Duration, type ManagedRuntime } from "effect"
import type { HealthPort, MetricsPort, NotifyPort, RouterPort } from "@flux/application"
import { healthCheck, monitorStep, notify, shiftTraffic } from "@flux/application"
import type { DeploymentActivities } from "./types.ts"

/** Services the worker's ManagedRuntime must provide (the 4 ports). */
export type AppServices = HealthPort | MetricsPort | NotifyPort | RouterPort

/**
 * Bridge Effect → Promise for Temporal (ARCHITECTURE.md D7).
 *
 * Activities are built once at worker startup around a single `ManagedRuntime`
 * (holding the composed adapter Layers), instead of re-providing Layers per
 * call. Each activity is a thin `runtime.runPromise(useCase(...))`.
 *
 * Note: a failed Effect currently rejects the Promise, so Temporal retries per
 * the activity retry policy. Mapping typed `Exit`/`Cause` to non-retryable
 * `ApplicationFailure` for business errors is scheduled for N2 (ARCHITECTURE.md §5).
 */
export const createActivities = (
  runtime: ManagedRuntime.ManagedRuntime<AppServices, never>
): DeploymentActivities => ({
  healthCheck: (params) => runtime.runPromise(healthCheck(params)),

  setTrafficWeight: (params) => runtime.runPromise(shiftTraffic(params)),

  monitorStep: (params) =>
    runtime.runPromise(
      monitorStep({
        service: params.service,
        version: params.version,
        window: Duration.millis(params.windowMs),
        thresholds: params.thresholds
      })
    ),

  notify: (params) => runtime.runPromise(notify(params))
})
