import { Schema } from "effect"

/**
 * A point-in-time snapshot of the metrics flux watches during a canary step.
 * Collected by the metrics adapter (Prometheus) and evaluated against the
 * configured thresholds — see `evaluateThresholds`.
 */
export const MetricsSnapshot = Schema.Struct({
  /** Fraction of failing requests in the window, `0..1` (e.g. `0.012` = 1.2%). */
  errorRate: Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0)),
  /** 99th percentile request latency in milliseconds. */
  p99LatencyMs: Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0))
})

export type MetricsSnapshot = typeof MetricsSnapshot.Type
