import { Duration, Effect, Option, Schedule, Stream } from "effect"
import { evaluateThresholds, type MetricReadings, type MetricRule, type Reading, type ThresholdEvaluation } from "@flux/domain"
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
  /** Judged on `outcomes` rather than on a query. */
  readonly outcomeRule?: { readonly name: string; readonly max: number } | undefined
  /** Verdicts the workflow has received, as of the start of this window. */
  readonly outcomes?: { readonly total: number; readonly failures: number } | undefined
}) {
  yield* Effect.annotateCurrentSpan({ "flux.service": params.service, "flux.version": params.version })
  const metrics = yield* MetricsPort

  // A rule that declares `sampleSize` costs a second query, issued alongside
  // the first: the two are independent, and the port's RequestResolver folds
  // any PromQL shared across rules into one backend fetch either way.
  const pollAndEvaluate = Effect.gen(function*() {
    const readings: Record<string, Reading> = {}
    yield* Effect.forEach(
      params.rules,
      (rule) =>
        Effect.zip(
          metrics.query(rule.query),
          rule.sampleSize === undefined
            ? Effect.succeed(undefined)
            : metrics.query(rule.sampleSize),
          { concurrent: true }
        ).pipe(Effect.map(([value, sampleSize]) => {
          readings[rule.name] = { value, sampleSize }
        })),
      { concurrency: "unbounded" }
    )
    // A pushed verdict is a reading like any other, which is the point: once it
    // carries its own count it goes through the same interval as a scraped
    // rate, and zero verdicts lands on `Inconclusive` rather than on a tidy
    // 0% failure rate. Nothing observed is not the same as nothing wrong.
    if (params.outcomeRule !== undefined) {
      const tally = params.outcomes ?? { total: 0, failures: 0 }
      readings[params.outcomeRule.name] = {
        value: tally.total === 0 ? 0 : tally.failures / tally.total,
        sampleSize: tally.total
      }
    }

    const rules = params.outcomeRule === undefined ? params.rules : [...params.rules, params.outcomeRule]
    return evaluateThresholds(readings as MetricReadings, rules)
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
