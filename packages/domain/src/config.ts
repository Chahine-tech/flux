import { Schema } from "effect"
import { DurationFromShorthand } from "./duration.ts"

/** Non-empty identifier (service name, image version, …). */
const NonEmptyString = Schema.String.check(Schema.isMinLength(1))

/**
 * Failure budget for a canary step. A step is rolled back as soon as an
 * observed metric crosses one of these limits (see `evaluateThresholds`).
 */
export const Thresholds = Schema.Struct({
  /** Max tolerated error rate as a fraction, `0..1`. */
  maxErrorRate: Schema.Finite.check(Schema.isBetween({ minimum: 0, maximum: 1 })),
  /** Max tolerated p99 latency in milliseconds. */
  maxP99LatencyMs: Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0))
})
export type Thresholds = typeof Thresholds.Type

/** One stage of a progressive rollout: shift `percent` traffic, then watch. */
export const CanaryStep = Schema.Struct({
  /** Share of traffic routed to the new version at this step, `0..100`. */
  percent: Schema.Finite.check(Schema.isBetween({ minimum: 0, maximum: 100 })),
  /** How long to monitor metrics before advancing. */
  monitorDuration: DurationFromShorthand,
  /** Whether the step pauses for manual approval before advancing. */
  requiresApproval: Schema.Boolean,
  /** How long to wait for approval before timing out (only if `requiresApproval`). */
  approvalTimeout: Schema.optionalKey(DurationFromShorthand)
})
export type CanaryStep = typeof CanaryStep.Type

/**
 * Deployment strategies, modelled as a discriminated union so new strategies
 * (blue/green, rolling…) can be added as tagged members without touching call
 * sites that pattern-match on `_tag`.
 */
export const CanaryStrategy = Schema.TaggedStruct("canary", {
  steps: Schema.NonEmptyArray(CanaryStep)
})
export type CanaryStrategy = typeof CanaryStrategy.Type

export const Strategy = Schema.Union([CanaryStrategy])
export type Strategy = typeof Strategy.Type

/** The full, normalized configuration for a single deployment run. */
export const DeploymentConfig = Schema.Struct({
  service: NonEmptyString,
  /** Version being rolled out. */
  version: NonEmptyString,
  /** Version to roll back to on failure. */
  previousVersion: NonEmptyString,
  strategy: Strategy,
  thresholds: Thresholds
})
export type DeploymentConfig = typeof DeploymentConfig.Type
