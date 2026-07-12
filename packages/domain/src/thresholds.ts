import { Schema } from "effect"
import type { MetricRule } from "./config.ts"

/** Which metric was breached, its observed value, and the limit it crossed. */
export const ThresholdBreach = Schema.Struct({
  metric: Schema.String,
  observed: Schema.Finite,
  limit: Schema.Finite
})
export type ThresholdBreach = typeof ThresholdBreach.Type

/**
 * Result of comparing metric readings against the rules. A plain discriminated
 * union (no Effect) so it stays trivially pure and property-testable, and so
 * workflows can import the type with `import type`.
 */
export type ThresholdEvaluation =
  | { readonly _tag: "Within" }
  | { readonly _tag: "Breached"; readonly breaches: readonly [ThresholdBreach, ...ThresholdBreach[]] }

/** A metric reading keyed by rule name. */
export type MetricReadings = Readonly<Record<string, number>>

/**
 * Core business rule: decide whether the observed readings stay within budget.
 * A rule breaches when its reading exceeds `max`. Pure and total — the heart of
 * the auto-rollback decision.
 */
export const evaluateThresholds = (
  readings: MetricReadings,
  rules: ReadonlyArray<MetricRule>
): ThresholdEvaluation => {
  const breaches: ThresholdBreach[] = []

  for (const rule of rules) {
    const observed = readings[rule.name]
    if (observed !== undefined && observed > rule.max) {
      breaches.push({ metric: rule.name, observed, limit: rule.max })
    }
  }

  const [first, ...rest] = breaches
  return first === undefined
    ? { _tag: "Within" }
    : { _tag: "Breached", breaches: [first, ...rest] }
}
