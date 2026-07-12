import { Duration, Effect, type ManagedRuntime, Schema, Tracer } from "effect"
import type { HealthPort, MetricsPort, NotifyPort, RouterPort } from "@flux/application"
import { healthCheck, monitorStep, notify, shiftTraffic } from "@flux/application"
import { Context as ActivityContext } from "@temporalio/activity"
import { ApplicationFailure } from "@temporalio/common"
import { recordOutcome, recordTrafficShift } from "../metrics.ts"
import { type FluxError, toApplicationFailure } from "./activity-error.ts"
import { HealthCheckParams, MonitorStepParams, NotifyParams, SetTrafficWeightParams } from "./schemas.ts"
import type { DeploymentActivities } from "./types.ts"

/** Services the worker's ManagedRuntime must provide (the 4 ports). */
export type AppServices = HealthPort | MetricsPort | NotifyPort | RouterPort

/**
 * Distributed tracing (N2, path B): every activity of one deployment shares a
 * trace derived from the workflow run id, so the whole deployment shows up as a
 * single trace in Jaeger. The activity self-derives the trace context from its
 * own Temporal `Context` — no OpenTelemetry SDK, no interceptors, no header
 * plumbing. `Effect.withParentSpan` re-parents the use case's own spans onto it.
 */
const deploymentTraceParent = (): Tracer.ExternalSpan | undefined => {
  let runId: string | undefined
  try {
    runId = ActivityContext.current().info.workflowExecution?.runId
  } catch {
    return undefined // not running inside an activity (e.g. unit tests)
  }
  if (runId === undefined) {
    return undefined
  }
  const traceId = runId.replace(/-/g, "").padEnd(32, "0").slice(0, 32)
  return Tracer.externalSpan({ traceId, spanId: traceId.slice(0, 16) })
}

const linkToDeployment = <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> => {
  const parent = deploymentTraceParent()
  return parent === undefined ? effect : effect.pipe(Effect.withParentSpan(parent))
}

/**
 * Bridge Effect → Promise for Temporal.
 *
 * Activities are built once at worker startup around a single `ManagedRuntime`.
 * Each activity (1) validates its wire payload against a Schema, then (2) runs
 * the use case, mapping both invalid-input and typed use-case errors to a
 * Temporal `ApplicationFailure` so failures cross the boundary with meaning.
 */
export const createActivities = (
  runtime: ManagedRuntime.ManagedRuntime<AppServices, never>
): DeploymentActivities => {
  // Decode the wire payload, then run the use case — mapping both failure kinds.
  const validated = <From, DecodeError, A, E extends FluxError>(
    decode: (raw: unknown) => Effect.Effect<From, DecodeError>,
    raw: unknown,
    toEffect: (params: From) => Effect.Effect<A, E, AppServices>
  ): Promise<A> =>
    runtime.runPromise(
      decode(raw).pipe(
        Effect.mapError((error) =>
          ApplicationFailure.nonRetryable(`invalid activity input: ${String(error)}`, "InvalidActivityInput")
        ),
        Effect.flatMap((params) => Effect.mapError(toEffect(params), toApplicationFailure))
      )
    )

  const decodeHealth = Schema.decodeUnknownEffect(HealthCheckParams)
  const decodeShift = Schema.decodeUnknownEffect(SetTrafficWeightParams)
  const decodeMonitor = Schema.decodeUnknownEffect(MonitorStepParams)
  const decodeNotify = Schema.decodeUnknownEffect(NotifyParams)

  return {
    healthCheck: (params) => validated(decodeHealth, params, (p) => linkToDeployment(healthCheck(p))),

    setTrafficWeight: (params) =>
      validated(decodeShift, params, (p) =>
        linkToDeployment(shiftTraffic(p).pipe(Effect.tap(() => recordTrafficShift)))),

    monitorStep: (params) =>
      validated(decodeMonitor, params, (p) =>
        linkToDeployment(monitorStep({
          service: p.service,
          version: p.version,
          window: Duration.millis(p.windowMs),
          pollInterval: Duration.millis(p.pollIntervalMs),
          rules: p.rules
        }))),

    notify: (params) => validated(decodeNotify, params, (p) => linkToDeployment(notify(p))),

    // Metric updates cannot fail (in-memory), so they skip validation/mapping.
    recordOutcome: (outcome) => runtime.runPromise(recordOutcome(outcome))
  }
}
