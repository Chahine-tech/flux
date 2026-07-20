import { Duration, Effect, type ManagedRuntime, Schema } from "effect"
import type { HealthPort, MetricsPort, NotifyPort, RouterPort } from "@flux/application"
import { healthCheck, monitorStep, notify, readRouterState, shiftTraffic } from "@flux/application"
import { Context as ActivityContext } from "@temporalio/activity"
import { ApplicationFailure } from "@temporalio/common"
import { recordOutcome, recordTrafficShift } from "../metrics.ts"
import { currentActivityTraceParent } from "../tracing/activity-interceptor.ts"
import { type FluxError, toApplicationFailure } from "./activity-error.ts"
import { HealthCheckParams, MonitorStepParams, NotifyParams, ReadRouterStateParams, SetTrafficWeightParams } from "./schemas.ts"
import type { DeploymentActivities } from "./types.ts"

/** Services the worker's ManagedRuntime must provide (the 4 ports). */
export type AppServices = HealthPort | MetricsPort | NotifyPort | RouterPort

/** The deployment's business id (`dep-service-…`), from the activity's Temporal context. */
const currentDeploymentId = (): string | undefined => {
  try {
    return ActivityContext.current().info.workflowExecution?.workflowId
  } catch {
    return undefined // not inside an activity (unit tests)
  }
}

/**
 * Distributed tracing (D24, replaces N2/voie B): every activity of one
 * deployment shares the trace the CLI/control-plane started, propagated
 * through Temporal headers by a client + activity interceptor pair (see
 * `../tracing/`). `Effect.withParentSpan` re-parents the use case's own spans
 * onto it. Falls back to no parent when the header is absent (e.g. unit
 * tests, or a client that predates D24).
 *
 * The same wrapper also stamps a correlation id onto every log line the use
 * case emits (D29): `Effect.annotateLogs` — v4's successor to FiberRef for
 * "a value on every log line without threading it" — carries `flux.deployment`
 * down the fiber tree, so an activity's logs say which deployment they belong
 * to for free.
 */
/** Stamp `flux.deployment` onto every log line the effect emits (D29). Exported for its test. */
export const withDeploymentLog = <A, E, R>(
  deploymentId: string | undefined,
  effect: Effect.Effect<A, E, R>
): Effect.Effect<A, E, R> =>
  deploymentId === undefined ? effect : effect.pipe(Effect.annotateLogs({ "flux.deployment": deploymentId }))

const linkToDeployment = <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> => {
  const correlated = withDeploymentLog(currentDeploymentId(), effect)
  const parent = currentActivityTraceParent()
  return parent === undefined ? correlated : correlated.pipe(Effect.withParentSpan(parent))
}

const HEARTBEAT_INTERVAL = Duration.seconds(10)

/**
 * Wrap a long-running activity effect (monitoring, N4/D16) with:
 * - a heartbeat daemon, so Temporal knows the activity is alive and can time it
 *   out / cancel it (and so it can be retried on a fresh worker if this one dies);
 * - cancellation: `Context.current().cancelled` rejects with `CancelledFailure`
 *   when the workflow cancels the activity (an abort), which we surface raw —
 *   this races the work, so an in-flight monitor stops at once instead of running
 *   to the end of its window.
 *
 * The cancellation must already be past the error-mapping to `ApplicationFailure`
 * so `CancelledFailure` reaches Temporal unchanged and is recognised as a cancel.
 */
const withHeartbeatAndCancellation = <A, E>(
  effect: Effect.Effect<A, E, AppServices>
): Effect.Effect<A, E, AppServices> =>
  Effect.gen(function*() {
    const ctx = ActivityContext.current()
    yield* Effect.forkScoped(
      Effect.sync(() => ctx.heartbeat()).pipe(Effect.delay(HEARTBEAT_INTERVAL), Effect.forever)
    )
    const cancelled = Effect.tryPromise({
      try: () => ctx.cancelled as Promise<never>,
      // CancelledFailure — thrown raw; typed as E only to satisfy `raceFirst`.
      catch: (error) => error as E
    })
    return yield* Effect.raceFirst(effect, cancelled)
  }).pipe(Effect.scoped)

/** A Schema decode failure crossing into Temporal — non-retryable and tagged. */
const invalidActivityInput = (error: unknown): ApplicationFailure =>
  ApplicationFailure.nonRetryable(`invalid activity input: ${String(error)}`, "InvalidActivityInput")

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
        Effect.mapError(invalidActivityInput),
        Effect.flatMap((params) => Effect.mapError(toEffect(params), toApplicationFailure))
      )
    )

  const decodeHealth = Schema.decodeUnknownEffect(HealthCheckParams)
  const decodeShift = Schema.decodeUnknownEffect(SetTrafficWeightParams)
  const decodeMonitor = Schema.decodeUnknownEffect(MonitorStepParams)
  const decodeNotify = Schema.decodeUnknownEffect(NotifyParams)
  const decodeReadState = Schema.decodeUnknownEffect(ReadRouterStateParams)

  return {
    healthCheck: (params) => validated(decodeHealth, params, (p) => linkToDeployment(healthCheck(p))),

    setTrafficWeight: (params) =>
      validated(decodeShift, params, (p) =>
        linkToDeployment(shiftTraffic(p).pipe(Effect.tap(() => recordTrafficShift)))),

    monitorStep: (params) =>
      runtime.runPromise(
        decodeMonitor(params).pipe(
          Effect.mapError(invalidActivityInput),
          Effect.flatMap((p) =>
            withHeartbeatAndCancellation(
              Effect.mapError(
                linkToDeployment(monitorStep({
                  service: p.service,
                  version: p.version,
                  window: Duration.millis(p.windowMs),
                  pollInterval: Duration.millis(p.pollIntervalMs),
                  rules: p.rules
                })),
                toApplicationFailure
              )
            ))
        )
      ),

    notify: (params) => validated(decodeNotify, params, (p) => linkToDeployment(notify(p))),

    readRouterState: (params) =>
      validated(decodeReadState, params, (p) => linkToDeployment(readRouterState(p.service))),

    // Metric updates cannot fail (in-memory), so they skip validation/mapping.
    recordOutcome: (outcome) => runtime.runPromise(recordOutcome(outcome))
  }
}
