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
  /** Upper bound: a breach is `observed > max`. */
  max: Schema.Finite,
  /**
   * PromQL returning how many observations produced `query`, when the metric is
   * a proportion. Supplying it switches the rule from a bare comparison to one
   * that can answer "not enough evidence yet" instead of promoting on noise.
   *
   * Only meaningful for a rate in `[0, 1]`: the interval behind this is for
   * proportions, and a latency handed a sample size falls back to the plain
   * comparison rather than producing a confident wrong number.
   */
  sampleSize: Schema.optional(NonEmptyString),
  /**
   * PromQL for the standard deviation, when the metric is an average rather
   * than a rate. Supplying it (alongside `sampleSize`) switches the rule to a
   * Student interval on the mean.
   *
   * A mean needs its spread and a rate does not: two runs averaging $0.10 over
   * 40 tasks, one ranging $0.09 to $0.11 and the other $0.01 to $0.50, say
   * completely different things about the next task.
   */
  stdDev: Schema.optional(NonEmptyString),
  /**
   * What a confirmed breach of this rule should do. Defaults to rolling back.
   *
   * `pause` stops the rollout where it is, traffic untouched, and waits for a
   * person. It is for limits whose breach is a judgement call rather than a
   * fault: a version that is better on every technical measure and costs 38%
   * more per unit of work has not regressed, it has presented a tradeoff, and
   * no threshold in a config file is entitled to settle that on its own.
   *
   * A rollback rule outranks a pause rule when both breach at once: a genuine
   * technical regression is not up for discussion.
   */
  onBreach: Schema.optional(Schema.Literals(["rollback", "pause"]))
})
export type MetricRule = typeof MetricRule.Type

/**
 * A rule fed by verdicts pushed in from outside rather than by a query.
 *
 * Some things are only known later. Whether an agent's task succeeded is
 * settled when the pull request merges, when the suite goes green, when a
 * person accepts the work. There is no gauge to scrape at the moment of the
 * decision, so the decision has to be able to wait for the answer to arrive.
 *
 * No `query`, because nothing is asked; no `sampleSize` either, because the
 * count is exactly the number of verdicts received, which the workflow already
 * knows. The limit is a failure rate, so the interval from `confidence.ts`
 * applies and the same three answers follow: too few verdicts is undecided, not
 * healthy.
 */
export const OutcomeRule = Schema.Struct({
  name: NonEmptyString,
  max: Schema.Finite,
  /** As on `MetricRule`: roll back on breach, or stop and ask. */
  onBreach: Schema.optional(Schema.Literals(["rollback", "pause"]))
})
export type OutcomeRule = typeof OutcomeRule.Type

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

/**
 * Blue/green: the new version is deployed alongside the old, health-checked,
 * then traffic flips 100% at once (no split) after an optional approval. It
 * bakes for `bakeDuration`; a breach flips back instantly, since the old
 * version was never scaled down. The counterpoint to canary's gradual shift —
 * faster cutover and rollback, no intermediate exposure, but all-or-nothing.
 */
export const BlueGreenStrategy = Schema.TaggedStruct("blue-green", {
  /** How long to monitor after the flip before declaring success. */
  bakeDuration: DurationFromShorthand,
  /** Whether the flip pauses for manual approval first. */
  requiresApproval: Schema.Boolean,
  /** How long to wait for that approval before timing out. */
  approvalTimeout: Schema.optionalKey(DurationFromShorthand)
})
export type BlueGreenStrategy = typeof BlueGreenStrategy.Type

export const Strategy = Schema.Union([CanaryStrategy, BlueGreenStrategy])
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
