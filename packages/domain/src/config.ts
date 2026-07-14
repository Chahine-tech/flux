import { Schema } from "effect"
import { DurationFromShorthand } from "./duration.ts"

/** Non-empty identifier (service name, image version, …). */
const NonEmptyString = Schema.String.check(Schema.isMinLength(1))

/**
 * A safe service/version identifier. These values end up interpolated into
 * nginx `upstream` blocks and PromQL label matchers, so the charset is locked
 * down (alphanumerics plus `.`, `_`, `-`, starting and ending alphanumeric) —
 * a name like `api {}\nserver evil` or `v1"}` must be rejected at the boundary,
 * not escaped downstream.
 */
export const Identifier = Schema.String.check(
  Schema.isPattern(/^[a-zA-Z0-9]([a-zA-Z0-9._-]{0,62}[a-zA-Z0-9])?$/)
)
export type Identifier = typeof Identifier.Type

/**
 * One rule in the failure budget: watch the metric produced by `query` and
 * roll back if the observed value exceeds `max`. Modelling thresholds as a list
 * of PromQL-backed rules (rather than two fixed fields) makes them arbitrary
 * custom metrics — and lets two rules share a query, which is where the metrics
 * adapter's RequestResolver deduplicates the fetch.
 */
export const MetricRule = Schema.Struct({
  /** Human-facing name, used as the reading key and breach label. */
  name: NonEmptyString,
  /** The PromQL expression to evaluate. */
  query: NonEmptyString,
  /** Upper bound — a breach is `observed > max`. */
  max: Schema.Finite
})
export type MetricRule = typeof MetricRule.Type

/** The failure budget for a deployment: a non-empty list of metric rules. */
export const Thresholds = Schema.NonEmptyArray(MetricRule)
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
  service: Identifier,
  /** Version being rolled out. */
  version: Identifier,
  /** Version to roll back to on failure. */
  previousVersion: Identifier,
  strategy: Strategy,
  thresholds: Thresholds
})
export type DeploymentConfig = typeof DeploymentConfig.Type
