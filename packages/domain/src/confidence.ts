/**
 * How sure a reading actually makes us, which is the question a bare threshold
 * skips.
 *
 * A canary over HTTP sees thousands of requests in a five minute window, so an
 * error rate of 1% is 1%, and comparing it to a limit is sound. A canary over a
 * workload that produces tens of observations rather than thousands is a
 * different problem wearing the same shape: 1 failure in 30 reads as 3.3%, and
 * 3.3% against a 5% limit reads as "healthy, promote". It is nothing of the
 * kind. The true rate consistent with that sample runs from well under 1% to
 * roughly 17%, so the reading is compatible both with a version far better than
 * the limit and with one three times worse. Promoting on it is not a decision,
 * it is a coin flip with a number written on it.
 *
 * The worst case is the one that looks best. **Zero failures in 20 observations
 * proves nothing**: the interval still reaches past 16%, because twenty tries
 * is simply not enough to have met a 5% event. A threshold comparison reports
 * that sample as perfect.
 *
 * So the comparison needs the sample size, and a rate alone does not carry it:
 * 0.02 over 50 and 0.02 over 50,000 are the same number and not the same
 * information.
 *
 * **Wilson rather than the textbook interval.** The normal approximation
 * (`p ± z·sqrt(p(1-p)/n)`) is the one everybody writes down and it fails
 * exactly where this is needed: on small `n` it drifts outside `[0, 1]`, and on
 * zero observed failures it collapses to the single point `[0, 0]`, announcing
 * certainty precisely where there is none. Wilson's interval stays inside the
 * unit range and keeps its width when the count is zero, which is the case that
 * matters most here.
 */

/** A two-sided interval of plausible values for a proportion. */
export interface Interval {
  readonly lower: number
  readonly upper: number
}

/** 95% two-sided. The conventional choice, named rather than left as 1.96. */
export const Z_95 = 1.959963984540054

const clamp01 = (value: number): number => (value < 0 ? 0 : value > 1 ? 1 : value)

/**
 * Wilson score interval for `successes` out of `total`.
 *
 * With `total` at zero the answer is the whole range: no observations support
 * no conclusion, and saying so is the point of this module rather than an edge
 * case to be tidied away.
 */
export const wilsonInterval = (successes: number, total: number, z: number = Z_95): Interval => {
  if (!Number.isFinite(total) || total <= 0) return { lower: 0, upper: 1 }

  const n = total
  const k = Math.min(Math.max(successes, 0), n)
  const p = k / n
  const zz = z * z
  const denominator = 1 + zz / n
  const centre = (p + zz / (2 * n)) / denominator
  const margin = (z / denominator) * Math.sqrt((p * (1 - p)) / n + zz / (4 * n * n))

  return { lower: clamp01(centre - margin), upper: clamp01(centre + margin) }
}

/**
 * What a sample lets us say about a limit. Three answers, because two is the
 * error this module exists to correct: a rate that has not crossed the limit is
 * not the same claim as a rate we are confident sits below it.
 *
 * - `above`: the whole interval clears the limit, so the breach is real.
 * - `below`: the whole interval sits under it, so the version has earned its
 *   promotion.
 * - `unknown`: the interval straddles the limit. The sample is consistent with
 *   both, and the honest report is that we do not know yet.
 */
export type Verdict = "above" | "below" | "unknown"

/**
 * Compare a proportion to a limit, given how many observations produced it.
 *
 * `observed` is the rate, not the count, because that is what a PromQL rule
 * returns; the count is recovered as `observed * total`. Note the asymmetry in
 * the comparisons: a breach needs the lower bound **strictly above** the limit,
 * matching the plain rule that a breach is `observed > max`, while clearing
 * needs the upper bound at or under it.
 */
export const compareToLimit = (observed: number, total: number, limit: number, z: number = Z_95): Verdict => {
  const { lower, upper } = wilsonInterval(observed * total, total, z)
  if (lower > limit) return "above"
  if (upper <= limit) return "below"
  return "unknown"
}

/**
 * The same question for a metric that is an average rather than a rate.
 *
 * Cost per task is the one that motivated this, and it is the measure no
 * deployment tool looks at: "v2 is better on every technical metric and costs
 * 38% more per unit of work" is a real outcome and not one a latency budget
 * catches. But an average is not a proportion, so Wilson does not apply, and a
 * mean needs something a rate does not: its spread. Two runs averaging $0.10
 * over 40 tasks, one ranging $0.09 to $0.11 and the other $0.01 to $0.50, carry
 * completely different evidence about the next task, and the mean alone cannot
 * tell them apart.
 *
 * **Student, not the normal approximation.** With the standard deviation
 * estimated from the same small sample, using `z` makes the interval too narrow
 * and concludes too early, which is precisely the failure this module exists to
 * prevent. The table below is exact for the sample sizes where it matters; past
 * 30 degrees of freedom the correction `z(1 + (z² + 1) / 4df)` is within a
 * fraction of a percent of the real value and converges on `z`, as it should.
 */
const T_95: ReadonlyArray<number> = [
  12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365, 2.306, 2.262, 2.228,
  2.201, 2.179, 2.160, 2.145, 2.131, 2.120, 2.110, 2.101, 2.093, 2.086,
  2.080, 2.074, 2.069, 2.064, 2.060, 2.056, 2.052, 2.048, 2.045, 2.042
]

/** Two-sided 95% critical value for `df` degrees of freedom. */
export const tCritical = (df: number): number => {
  if (df < 1) return Number.POSITIVE_INFINITY
  const exact = T_95[Math.min(df, T_95.length) - 1]
  if (df <= T_95.length) return exact as number
  return Z_95 * (1 + (Z_95 * Z_95 + 1) / (4 * df))
}

/**
 * Confidence interval for a mean, from the sample's own mean, spread and size.
 *
 * Fewer than two observations gives the unbounded interval: one measurement has
 * no spread to estimate, so it supports no statement about the next one. That
 * is the honest answer rather than an edge case, exactly as with `total = 0`
 * above.
 */
export const meanInterval = (mean: number, stdDev: number, total: number): Interval => {
  if (!Number.isFinite(total) || total < 2 || !Number.isFinite(stdDev) || stdDev < 0) {
    return { lower: Number.NEGATIVE_INFINITY, upper: Number.POSITIVE_INFINITY }
  }
  const margin = tCritical(total - 1) * (stdDev / Math.sqrt(total))
  return { lower: mean - margin, upper: mean + margin }
}

/** `compareToLimit` for a mean: same three answers, different interval. */
export const compareMeanToLimit = (mean: number, stdDev: number, total: number, limit: number): Verdict => {
  const { lower, upper } = meanInterval(mean, stdDev, total)
  if (lower > limit) return "above"
  if (upper <= limit) return "below"
  return "unknown"
}
