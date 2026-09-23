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
