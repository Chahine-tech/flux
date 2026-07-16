import type { Duration } from "effect"
import { Schema } from "effect"
import type { MetricRule } from "@flux/domain"
import { HealthCheckFailed } from "@flux/domain"
import {
  healthCheck,
  MetricsUnavailable,
  type Notification,
  NotifyFailed,
  monitorStep,
  notify,
  RouterUnavailable,
  type SetTrafficWeightParams,
  shiftTraffic
} from "@flux/application"
import { Activity } from "effect/unstable/workflow"

/**
 * The four Temporal activities of `deployment.workflow.ts`, reimplemented as
 * `Activity.make` around the exact same `@flux/application` use cases (D23).
 * There is no D6/D7-shaped bridge here: an `Activity`'s `execute` is an
 * ordinary `Effect`, so the port requirement (`HealthPort`, `RouterPort`, …)
 * is just provided by whatever layer wraps the workflow — no
 * `ManagedRuntime.runPromise` boundary, no Promise round-trip, no separate
 * "worker process". That absence is itself the comparison's first datapoint.
 */

const ThresholdEvaluationSchema = Schema.Union([
  Schema.TaggedStruct("Within", {}),
  Schema.TaggedStruct("Breached", {
    breaches: Schema.NonEmptyArray(Schema.Struct({
      metric: Schema.String,
      observed: Schema.Finite,
      limit: Schema.Finite
    }))
  })
])

export const healthCheckActivity = (params: { readonly service: string; readonly version: string }) =>
  Activity.make({
    name: "healthCheck",
    error: HealthCheckFailed,
    execute: healthCheck(params)
  })

/**
 * `Activity.make`'s durability/memoization keys on `name` (+ retry attempt),
 * not on call order — unlike Temporal, where each `proxyActivities()` call is
 * distinguished by its position in the event history. Calling
 * `shiftTraffic`/`monitorStep` once per canary step with the *same* literal
 * name makes every later step replay the FIRST step's cached result instead
 * of re-running — a sharp, non-obvious edge found empirically (see D23), so
 * every call site must pass a name unique within the execution.
 */
export const shiftTrafficActivity = (name: string, params: SetTrafficWeightParams) =>
  Activity.make({
    name,
    error: RouterUnavailable,
    execute: shiftTraffic(params)
  })

export const monitorStepActivity = (name: string, params: {
  readonly service: string
  readonly version: string
  readonly window: Duration.Duration
  readonly pollInterval: Duration.Duration
  readonly rules: ReadonlyArray<MetricRule>
}) =>
  Activity.make({
    name,
    success: ThresholdEvaluationSchema,
    error: MetricsUnavailable,
    execute: monitorStep(params)
  })

export const notifyActivity = (notification: Notification) =>
  Activity.make({
    name: "notify",
    error: NotifyFailed,
    execute: notify(notification)
  })
