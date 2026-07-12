import { type Duration, Effect } from "effect"
import { evaluateThresholds, type Thresholds, type ThresholdEvaluation } from "@flux/domain"
import type { MetricsUnavailable } from "../errors.ts"
import { MetricsPort } from "../ports/metrics.ts"

/**
 * Use case: collect the current metrics snapshot and evaluate it against the
 * failure budget. Combines the MetricsPort (I/O) with the pure domain rule
 * `evaluateThresholds` — the auto-rollback decision for a canary step.
 */
export const monitorStep = (params: {
  readonly service: string
  readonly version: string
  readonly window: Duration.Duration
  readonly thresholds: Thresholds
}): Effect.Effect<ThresholdEvaluation, MetricsUnavailable, MetricsPort> =>
  Effect.gen(function*() {
    const metrics = yield* MetricsPort
    const snapshot = yield* metrics.collect({
      service: params.service,
      version: params.version,
      window: params.window
    })
    return evaluateThresholds(snapshot, params.thresholds)
  })
