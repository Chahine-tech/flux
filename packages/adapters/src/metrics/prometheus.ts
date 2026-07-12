import { Duration, Effect, Layer, Schedule, Schema } from "effect"
import { HttpClient } from "effect/unstable/http"
import { type CollectParams, MetricsPort, MetricsUnavailable } from "@flux/application"
import type { MetricsSnapshot } from "@flux/domain"

/**
 * Prometheus metrics adapter — implements MetricsPort against the Prometheus
 * HTTP API (`GET /api/v1/query`). Requires an `HttpClient` in context, which
 * the composition root provides (FetchHttpClient / NodeHttpClient). Transient
 * HTTP failures are retried here with a short jittered backoff (ARCHITECTURE.md
 * D9 — technical retries live in the adapter, not the workflow).
 */

// --- Response schema (instant query, vector result) ---
// A sample's `value` is `[unixSeconds, "<stringified float>"]`.
const Sample = Schema.Struct({
  value: Schema.Tuple([Schema.Number, Schema.String])
})
export const QueryResponse = Schema.Struct({
  status: Schema.Literals(["success", "error"]),
  data: Schema.Struct({
    result: Schema.Array(Sample)
  })
})
export type QueryResponse = typeof QueryResponse.Type

// --- Pure helpers (unit-tested; full HTTP path covered by the e2e demo) ---

/** Render a Duration as a Prometheus range selector, e.g. `300s` (min 1s). */
export const promRange = (window: Duration.Duration): string => {
  const seconds = Math.max(1, Math.round(Duration.toSeconds(window)))
  return `${seconds}s`
}

/** Error rate as a fraction of 5xx over total requests for the service. */
export const errorRateQuery = (service: string, window: Duration.Duration): string => {
  const range = promRange(window)
  return (
    `sum(rate(http_requests_total{service="${service}",status=~"5.."}[${range}]))` +
    ` / sum(rate(http_requests_total{service="${service}"}[${range}]))`
  )
}

/** p99 request latency in milliseconds for the service. */
export const p99LatencyQuery = (service: string, window: Duration.Duration): string => {
  const range = promRange(window)
  return (
    `histogram_quantile(0.99, sum(rate(` +
    `http_request_duration_seconds_bucket{service="${service}"}[${range}]` +
    `)) by (le)) * 1000`
  )
}

/**
 * Extract the scalar value from an instant-query response.
 * An empty result set means "no data" — treated as `0` so a service with no
 * traffic yet does not trip a threshold. Non-finite values collapse to `0`.
 */
export const extractScalar = (response: QueryResponse): number => {
  const first = response.data.result[0]
  if (first === undefined) {
    return 0
  }
  const parsed = Number(first.value[1])
  return Number.isFinite(parsed) ? parsed : 0
}

// --- Layer ---

export interface PrometheusOptions {
  /** Base URL of the Prometheus server, e.g. `http://localhost:9090`. */
  readonly url: string
}

const retryPolicy = {
  schedule: Schedule.exponential(Duration.millis(200)).pipe(Schedule.jittered),
  times: 3
} as const

export const layer = (
  options: PrometheusOptions
): Layer.Layer<MetricsPort, never, HttpClient.HttpClient> =>
  Layer.effect(
    MetricsPort,
    Effect.gen(function*() {
      const client = yield* HttpClient.HttpClient

      const decodeResponse = Schema.decodeUnknownEffect(QueryResponse)

      const query = (promql: string): Effect.Effect<number, MetricsUnavailable> =>
        client.get(`${options.url}/api/v1/query`, { urlParams: { query: promql } }).pipe(
          Effect.flatMap((response) => response.json),
          Effect.flatMap(decodeResponse),
          Effect.map(extractScalar),
          Effect.retry(retryPolicy),
          Effect.mapError(
            (cause) =>
              new MetricsUnavailable({
                service: "prometheus",
                reason: cause instanceof Error ? cause.message : String(cause)
              })
          )
        )

      return {
        collect: (params: CollectParams): Effect.Effect<MetricsSnapshot, MetricsUnavailable> =>
          Effect.all(
            {
              errorRate: query(errorRateQuery(params.service, params.window)),
              p99LatencyMs: query(p99LatencyQuery(params.service, params.window))
            },
            { concurrency: 2 }
          )
      }
    })
  )
