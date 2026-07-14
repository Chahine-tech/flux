import { Duration, Effect, Option, Schedule, Stream } from "effect"
import { evaluateThresholds, type MetricReadings, type MetricRule, type ThresholdEvaluation } from "@flux/domain"
import { MetricsPort } from "../ports/metrics.ts"

const isBreached = (evaluation: ThresholdEvaluation): boolean => evaluation._tag === "Breached"

const within: ThresholdEvaluation = { _tag: "Within" }

/**
 * Use case: monitor a canary step as a `Stream`.
 *
 * Each poll evaluates every rule concurrently through the MetricsPort. Because
 * the queries hit the port together, its RequestResolver deduplicates any that
 * share a PromQL into a single backend fetch. The readings are compared to the
 * rules; monitoring polls every `pollInterval` for up to `window` and stops
 * early (`Stream.takeUntil`) the moment a rule breaches.
 */
export const monitorStep = Effect.fn("flux.monitorStep")(function*(params: {
  readonly service: string
  readonly version: string
  readonly window: Duration.Duration
  readonly pollInterval: Duration.Duration
  readonly rules: ReadonlyArray<MetricRule>
}) {
  yield* Effect.annotateCurrentSpan({ "flux.service": params.service, "flux.version": params.version })
  const metrics = yield* MetricsPort

  const pollAndEvaluate = Effect.gen(function*() {
    const readings: Record<string, number> = {}
    yield* Effect.forEach(
      params.rules,
      (rule) => metrics.query(rule.query).pipe(Effect.map((value) => {
        readings[rule.name] = value
      })),
      { concurrency: "unbounded" }
    )
    return evaluateThresholds(readings as MetricReadings, params.rules)
  })

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
