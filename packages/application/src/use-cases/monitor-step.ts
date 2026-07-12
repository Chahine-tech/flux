import { Duration, Effect, Option, Schedule, Stream } from "effect"
import { evaluateThresholds, type Thresholds, type ThresholdEvaluation } from "@flux/domain"
import type { MetricsUnavailable } from "../errors.ts"
import { MetricsPort } from "../ports/metrics.ts"

const isBreached = (evaluation: ThresholdEvaluation): boolean => evaluation._tag === "Breached"

const within: ThresholdEvaluation = { _tag: "Within" }

/**
 * Use case: monitor a canary step as a `Stream`.
 *
 * Rather than sleeping once then taking a single sample, this polls the metrics
 * every `pollInterval` for up to `window`, evaluates each snapshot against the
 * failure budget, and **stops early** the moment a breach is seen
 * (`Stream.takeUntil`). The outcome is the last evaluation: `Breached` if a poll
 * tripped a threshold, otherwise `Within` once the window elapses.
 */
export const monitorStep = (params: {
  readonly service: string
  readonly version: string
  readonly window: Duration.Duration
  readonly pollInterval: Duration.Duration
  readonly thresholds: Thresholds
}): Effect.Effect<ThresholdEvaluation, MetricsUnavailable, MetricsPort> =>
  Effect.gen(function*() {
    const metrics = yield* MetricsPort

    const pollAndEvaluate = metrics
      .collect({ service: params.service, version: params.version, window: params.window })
      .pipe(Effect.map((snapshot) => evaluateThresholds(snapshot, params.thresholds)))

    const windowMs = Duration.toMillis(params.window)
    const intervalMs = Math.max(1, Duration.toMillis(params.pollInterval))
    const maxPolls = Math.max(1, Math.floor(windowMs / intervalMs) + 1)

    const lastEvaluation = yield* Stream.fromEffectRepeat(pollAndEvaluate).pipe(
      Stream.schedule(Schedule.spaced(params.pollInterval)),
      Stream.take(maxPolls),
      Stream.takeUntil(isBreached),
      Stream.runLast
    )

    return Option.getOrElse(lastEvaluation, () => within)
  })
