import { Schema } from "effect"
import { Thresholds } from "@flux/domain"

/**
 * Schemas for the values that cross the Temporal wire into an activity.
 *
 * Temporal's payload converter runs in the workflow VM and cannot host the
 * Effect runtime, so wire-validation lives here — at the activity boundary,
 * where the typed value exists and Effect legitimately runs (see ARCHITECTURE
 * D8). A durable workflow can be replayed with a payload from an older code
 * version; decoding here turns such drift into a clear, non-retryable failure
 * instead of a garbage value flowing into a use case.
 */

const NonEmptyString = Schema.String.check(Schema.isMinLength(1))

export const HealthCheckParams = Schema.Struct({
  service: NonEmptyString,
  version: NonEmptyString
})

export const SetTrafficWeightParams = Schema.Struct({
  service: NonEmptyString,
  version: NonEmptyString,
  weight: Schema.Finite.check(Schema.isBetween({ minimum: 0, maximum: 100 }))
})

export const MonitorStepParams = Schema.Struct({
  service: NonEmptyString,
  version: NonEmptyString,
  windowMs: Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0)),
  pollIntervalMs: Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0)),
  rules: Thresholds
})

export const NotifyParams = Schema.Struct({
  kind: Schema.Literals(["started", "step-advanced", "rolled-back", "succeeded"]),
  service: NonEmptyString,
  message: Schema.String
})
