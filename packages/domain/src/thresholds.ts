import { Schema } from "effect"
import type { Thresholds } from "./config.ts"
import type { MetricsSnapshot } from "./metrics.ts"

/** Which metric was breached, its observed value, and the limit it crossed. */
export const ThresholdBreach = Schema.Struct({
  metric: Schema.Literals(["errorRate", "p99LatencyMs"]),
  observed: Schema.Finite,
  limit: Schema.Finite
})
export type ThresholdBreach = typeof ThresholdBreach.Type

/**
 * Result of comparing a metrics snapshot against the thresholds.
 * A plain discriminated union (no Effect) so it stays trivially pure and
 * property-testable, and so workflows can import the type with `import type`.
 */
export type ThresholdEvaluation =
  | { readonly _tag: "Within" }
  | { readonly _tag: "Breached"; readonly breaches: readonly [ThresholdBreach, ...ThresholdBreach[]] }

/**
 * Core business rule: decide whether an observed snapshot stays within budget.
 * Pure and total — the heart of the auto-rollback decision.
 */
export const evaluateThresholds = (
  snapshot: MetricsSnapshot,
  thresholds: Thresholds
): ThresholdEvaluation => {
  const breaches: ThresholdBreach[] = []

  if (snapshot.errorRate > thresholds.maxErrorRate) {
    breaches.push({
      metric: "errorRate",
      observed: snapshot.errorRate,
      limit: thresholds.maxErrorRate
    })
  }

  if (snapshot.p99LatencyMs > thresholds.maxP99LatencyMs) {
    breaches.push({
      metric: "p99LatencyMs",
      observed: snapshot.p99LatencyMs,
      limit: thresholds.maxP99LatencyMs
    })
  }

  const [first, ...rest] = breaches
  return first === undefined
    ? { _tag: "Within" }
    : { _tag: "Breached", breaches: [first, ...rest] }
}
