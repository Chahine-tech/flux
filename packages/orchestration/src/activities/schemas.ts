import { Schema } from "effect"
import { Identifier, OutcomeRule, ThresholdBreach, Thresholds } from "@flux/domain"

/**
 * Schemas for the values that cross the Temporal wire into an activity.
 *
 * Temporal's payload converter runs in the workflow VM and can't host the
 * Effect runtime, so wire-validation lives here, at the activity boundary,
 * where the typed value exists and Effect is allowed to run. A durable workflow
 * can be replayed with a payload from an older code version; decoding here turns
 * that drift into a clear, non-retryable failure instead of a garbage value
 * flowing into a use case.
 */

export const HealthCheckParams = Schema.Struct({
  service: Identifier,
  version: Identifier
})

export const SetTrafficWeightParams = Schema.Struct({
  service: Identifier,
  version: Identifier,
  weight: Schema.Finite.check(Schema.isBetween({ minimum: 0, maximum: 100 })),
  previousVersion: Schema.optionalKey(Identifier)
})

export const MonitorStepParams = Schema.Struct({
  service: Identifier,
  version: Identifier,
  windowMs: Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0)),
  pollIntervalMs: Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0)),
  rules: Thresholds,
  outcomeRule: Schema.optional(OutcomeRule),
  // Counts, so whole and non-negative. `failures <= total` is not stated here
  // because the workflow is the only writer and increments them together.
  outcomes: Schema.optional(Schema.Struct({
    total: Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0)),
    failures: Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0))
  }))
})

export const NotifyParams = Schema.Struct({
  kind: Schema.Literals(["started", "step-advanced", "rolled-back", "succeeded", "rollback-failed"]),
  service: Identifier,
  message: Schema.String
})

export const ReadRouterStateParams = Schema.Struct({
  service: Identifier
})

export const PostmortemParams = Schema.Struct({
  service: Identifier,
  version: Identifier,
  previousVersion: Identifier,
  atPercent: Schema.Finite.check(Schema.isBetween({ minimum: 0, maximum: 100 })),
  breaches: Schema.Array(ThresholdBreach)
})
