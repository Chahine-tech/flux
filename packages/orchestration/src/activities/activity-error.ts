import { Match } from "effect"
import { ApplicationFailure } from "@temporalio/common"
import { MetricsUnavailable, NotifyFailed, RouterUnavailable } from "@flux/application"
import { HealthCheckFailed, MetricsThresholdExceeded, RollbackFailed } from "@flux/domain"

/**
 * Where Effect's typed errors become Temporal failures — the point the whole
 * type-safe error story has to land (it is Effect's flagship value at the
 * Effect ⇄ Temporal seam).
 *
 * Each tagged error is dispatched with `Match.exhaustive` and mapped to an
 * `ApplicationFailure` whose `type` is the error's `_tag` (so a workflow can
 * match on it) and whose retryable flag reflects intent:
 *
 * - **Business outcomes** (health check failed, thresholds exceeded, rollback
 *   failed) are decisions, not transient faults → **non-retryable**.
 * - **Infrastructure faults** (metrics/router/notify unreachable) are transient
 *   → **retryable**, so Temporal's retry policy takes over.
 *
 * Defects (unexpected bugs) are deliberately NOT handled here: `Effect.mapError`
 * only touches the typed failure channel, so a defect surfaces as itself.
 */

/** Every typed error that can cross the activity boundary. */
export type FluxError =
  | HealthCheckFailed
  | MetricsThresholdExceeded
  | RollbackFailed
  | MetricsUnavailable
  | RouterUnavailable
  | NotifyFailed

export const toApplicationFailure = (error: FluxError): ApplicationFailure =>
  Match.value(error).pipe(
    Match.tag("HealthCheckFailed", (e) =>
      ApplicationFailure.nonRetryable(
        `health check failed for ${e.service} ${e.version}: ${e.reason}`,
        e._tag,
        e
      )),
    Match.tag("MetricsThresholdExceeded", (e) =>
      ApplicationFailure.nonRetryable(
        `thresholds exceeded for ${e.service} at ${e.atPercent}%`,
        e._tag,
        e
      )),
    Match.tag("RollbackFailed", (e) =>
      ApplicationFailure.nonRetryable(
        `rollback failed for ${e.service} to ${e.toVersion}: ${e.reason}`,
        e._tag,
        e
      )),
    Match.tag("MetricsUnavailable", (e) =>
      ApplicationFailure.retryable(`metrics unavailable for ${e.service}: ${e.reason}`, e._tag, e)),
    Match.tag("RouterUnavailable", (e) =>
      ApplicationFailure.retryable(`router unavailable for ${e.service}: ${e.reason}`, e._tag, e)),
    Match.tag("NotifyFailed", (e) =>
      ApplicationFailure.retryable(`notification failed on ${e.channel}: ${e.reason}`, e._tag, e)),
    Match.exhaustive
  )
