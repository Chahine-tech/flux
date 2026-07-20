import { Cron, Result, Schema } from "effect"

/**
 * Deployment windows (D28): a canary may only start inside an allowed
 * recurring time window, expressed as a **cron expression**. A wildcard minute
 * field makes the expression a window rather than an instant — `* 9-17 * * 1-5`
 * is "any weekday 09:00–17:59". `Cron.match` tests membership; `Cron.next`
 * tells the caller when the window next opens.
 *
 * The evaluator is pure and total (time is passed in, not read), the same
 * shape as `evaluateThresholds` — so it is unit- and property-testable, and
 * lives in the domain rather than in the STM admission transaction (D14),
 * which stays a pure concurrency invariant. The temporal gate is orthogonal.
 */

/** A cron expression validated as parseable at the boundary. */
export const DeploymentWindow = Schema.String.check(
  Schema.makeFilter(
    (value: string) => Result.isSuccess(Cron.parse(value)) || "a valid cron expression (5 or 6 fields)",
    { title: "DeploymentWindow" }
  )
)
export type DeploymentWindow = typeof DeploymentWindow.Type

/** Whether a deployment may start now, and if not, when the window next opens. */
export type WindowDecision =
  | { readonly _tag: "Open" }
  | { readonly _tag: "Closed"; readonly nextAllowed: Date }

const open: WindowDecision = { _tag: "Open" }

/**
 * Decide whether a deployment guarded by `window` may start at `now`.
 * An absent window is always open (unchanged behavior for callers that set
 * none). An unparseable window is treated as open too — validation belongs at
 * the schema boundary (`DeploymentWindow`), not here, and a pure evaluator
 * must not throw.
 *
 * Gotcha found at implementation: Effect's `Cron` is **second-precision**, and
 * a 5-field expression gets an implicit `second = 0`. So `Cron.match` tests a
 * single instant ("second 0 of a matching minute"), not membership in a range
 * — `* 9-17 * * 1-5` would match only at :00 seconds, rejecting a deploy at
 * 14:30:25. A deployment *window* wants minute-granularity membership, so we
 * floor `now` to the start of its minute before matching. Minute precision is
 * plenty for "business hours", and flooring seconds is timezone-independent.
 */
export const evaluateWindow = (window: string | undefined, now: Date): WindowDecision => {
  if (window === undefined) {
    return open
  }
  const parsed = Cron.parse(window)
  if (Result.isFailure(parsed)) {
    return open
  }
  const cron = parsed.success
  const flooredToMinute = new Date(now)
  flooredToMinute.setUTCSeconds(0, 0)
  return Cron.match(cron, flooredToMinute)
    ? open
    : { _tag: "Closed", nextAllowed: Cron.next(cron, flooredToMinute) }
}
