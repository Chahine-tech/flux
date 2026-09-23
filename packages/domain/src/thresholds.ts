import { Schema } from "effect"
import { compareToLimit, wilsonInterval } from "./confidence.ts"
import type { MetricRule } from "./config.ts"

/** Which metric was breached, its observed value, and the limit it crossed. */
export const ThresholdBreach = Schema.Struct({
  metric: Schema.String,
  observed: Schema.Finite,
  limit: Schema.Finite
})
export type ThresholdBreach = typeof ThresholdBreach.Type

/**
 * A rule that could not be decided: the sample is consistent with passing and
 * with breaching. The interval is carried along because "we do not know" is
 * only actionable next to how wide the not-knowing is.
 */
export const PendingReading = Schema.Struct({
  metric: Schema.String,
  observed: Schema.Finite,
  limit: Schema.Finite,
  sampleSize: Schema.Finite,
  lower: Schema.Finite,
  upper: Schema.Finite
})
export type PendingReading = typeof PendingReading.Type

/**
 * Result of comparing metric readings against the rules. A plain discriminated
 * union (no Effect) so it stays trivially pure and property-testable, and so
 * workflows can import the type with `import type`.
 *
 * `Inconclusive` is the third answer, and the reason this file changed. A rule
 * that has not crossed its limit is not the same claim as a rule we are
 * confident sits below it, and collapsing the two promotes on noise whenever
 * observations are scarce. See `confidence.ts` for why.
 */
export type ThresholdEvaluation =
  | { readonly _tag: "Within" }
  | { readonly _tag: "Breached"; readonly breaches: readonly [ThresholdBreach, ...ThresholdBreach[]] }
  | { readonly _tag: "Inconclusive"; readonly pending: readonly [PendingReading, ...PendingReading[]] }

/**
 * One metric reading: the value, and how many observations produced it when
 * that is known.
 *
 * The sample size lives here rather than in a second map because it is part of
 * the reading. A rate on its own cannot be compared to a limit honestly: 0.02
 * over 50 and 0.02 over 50,000 are the same number and carry completely
 * different amounts of evidence.
 */
export interface Reading {
  readonly value: number
  readonly sampleSize?: number | undefined
}

/** A metric reading keyed by rule name. */
export type MetricReadings = Readonly<Record<string, Reading>>

/**
 * Core business rule: decide whether the observed readings stay within budget.
 *
 * Two regimes, chosen per rule by whether the reading knows its sample size:
 *
 *   - **Without one**, a breach is `observed > max`, exactly as before. This is
 *     right for a metric backed by thousands of requests, where the sample is
 *     large enough that the distinction this module draws does not pay for
 *     itself, and it is the only thing that can be said when the denominator
 *     was never asked for.
 *
 *   - **With one**, the rate is treated as a proportion and compared through
 *     its confidence interval, which can answer that it does not yet know.
 *
 * The interval only applies to proportions, so a value outside `[0, 1]` falls
 * back to the plain comparison however the rule was configured. A latency in
 * milliseconds handed a sample size is a mistake, and inventing a Wilson
 * interval for it would turn that mistake into a confident number rather than a
 * wrong one.
 *
 * A confirmed breach outranks an undecided rule: if one metric is definitely
 * over budget, what the others have not settled yet does not change what to do.
 */
export const evaluateThresholds = (
  readings: MetricReadings,
  rules: ReadonlyArray<MetricRule>
): ThresholdEvaluation => {
  const breaches: ThresholdBreach[] = []
  const pending: PendingReading[] = []

  for (const rule of rules) {
    const reading = readings[rule.name]
    if (reading === undefined) continue
    const observed = reading.value
    const n = reading.sampleSize
    const isProportion = observed >= 0 && observed <= 1

    if (n === undefined || !isProportion) {
      if (observed > rule.max) breaches.push({ metric: rule.name, observed, limit: rule.max })
      continue
    }

    const verdict = compareToLimit(observed, n, rule.max)
    if (verdict === "above") {
      breaches.push({ metric: rule.name, observed, limit: rule.max })
    } else if (verdict === "unknown") {
      const { lower, upper } = wilsonInterval(observed * n, n)
      pending.push({ metric: rule.name, observed, limit: rule.max, sampleSize: n, lower, upper })
    }
  }

  const [firstBreach, ...restBreaches] = breaches
  if (firstBreach !== undefined) return { _tag: "Breached", breaches: [firstBreach, ...restBreaches] }

  const [firstPending, ...restPending] = pending
  return firstPending === undefined
    ? { _tag: "Within" }
    : { _tag: "Inconclusive", pending: [firstPending, ...restPending] }
}
