import { Schema } from "effect"

/**
 * Aggregations served by `GET /stats`, computed from the control plane's SQLite
 * read model (D12). These are the questions Temporal advanced visibility cannot
 * answer — it lists and filters deployments but cannot `GROUP BY` or average —
 * which is the whole reason the read model exists.
 */

const NonEmptyString = Schema.String.check(Schema.isMinLength(1))
const NonNegative = Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0))

/** Rollout statistics for a single service, across all its recorded deployments. */
export const ServiceStats = Schema.Struct({
  service: NonEmptyString,
  total: NonNegative,
  succeeded: NonNegative,
  rolledBack: NonNegative,
  aborted: NonNegative,
  failed: NonNegative,
  /** `rolledBack / total`, in `0..1` — the canary's demonstrated safety net. */
  rollbackRate: Schema.Finite.check(Schema.isBetween({ minimum: 0, maximum: 1 })),
  /** Mean wall-clock duration of a deployment for this service, in milliseconds. */
  meanDurationMs: NonNegative
})
export type ServiceStats = typeof ServiceStats.Type

export const StatsResponse = Schema.Struct({
  services: Schema.Array(ServiceStats)
})
export type StatsResponse = typeof StatsResponse.Type
