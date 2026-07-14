import { Schema } from "effect"

/**
 * Enable drift detection for a service (N4/D17): create or update a Temporal
 * Schedule that periodically checks the router actually routes `version` at 100%
 * and reconciles it if not.
 */

const NonEmptyString = Schema.String.check(Schema.isMinLength(1))

export const EnableDriftRequest = Schema.Struct({
  service: NonEmptyString,
  /** The version that should be receiving 100% of traffic. */
  version: NonEmptyString,
  /** How often the check runs, in milliseconds. */
  everyMs: Schema.Finite.check(Schema.isGreaterThanOrEqualTo(1000))
})
export type EnableDriftRequest = typeof EnableDriftRequest.Type

export const EnableDriftResponse = Schema.Struct({
  scheduleId: NonEmptyString
})
export type EnableDriftResponse = typeof EnableDriftResponse.Type
