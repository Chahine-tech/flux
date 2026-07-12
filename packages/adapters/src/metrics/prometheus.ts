import { Duration, Effect, Layer, Request, RequestResolver, Schedule, Schema } from "effect"
import { HttpClient } from "effect/unstable/http"
import { MetricsPort, MetricsUnavailable } from "@flux/application"

/**
 * Prometheus metrics adapter — implements MetricsPort against the Prometheus
 * HTTP API (`GET /api/v1/query`). Requires an `HttpClient` in context.
 *
 * Queries flow through a RequestResolver: a monitoring poll evaluates several
 * rules concurrently, so any that share a PromQL are batched together and the
 * resolver deduplicates them into a single backend fetch. Transient HTTP
 * failures are retried here with a short jittered backoff — technical retries
 * live in the adapter, not the workflow.
 */

// --- Response schema (instant query, vector result) ---
const Sample = Schema.Struct({
  value: Schema.Tuple([Schema.Number, Schema.String])
})
export const QueryResponse = Schema.Struct({
  status: Schema.Literals(["success", "error"]),
  data: Schema.Struct({ result: Schema.Array(Sample) })
})
export type QueryResponse = typeof QueryResponse.Type

// --- Default PromQL builders (used to seed a deployment's default rules) ---

/** Error rate as a fraction of 5xx over total requests for the service. */
export const errorRateQuery = (service: string): string =>
  `sum(rate(http_requests_total{service="${service}",status=~"5.."}[1m]))` +
  ` / sum(rate(http_requests_total{service="${service}"}[1m]))`

/** p99 request latency in milliseconds for the service. */
export const p99LatencyQuery = (service: string): string =>
  `histogram_quantile(0.99, sum(rate(` +
  `http_request_duration_seconds_bucket{service="${service}"}[1m]` +
  `)) by (le)) * 1000`

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

// --- Request + resolver (deduplicates identical queries in a batch) ---

interface PrometheusQuery extends Request.Request<number, MetricsUnavailable> {
  readonly _tag: "PrometheusQuery"
  readonly query: string
}
const PrometheusQuery = Request.tagged<PrometheusQuery>("PrometheusQuery")

/**
 * Build a resolver from a per-query fetcher. Within each batch, identical
 * PromQL strings are fetched once and their result fanned back out.
 */
export const makeQueryResolver = (
  fetch: (promql: string) => Effect.Effect<number, MetricsUnavailable>
): RequestResolver.RequestResolver<PrometheusQuery> =>
  RequestResolver.fromEffectTagged<PrometheusQuery>()({
    PrometheusQuery: (entries) =>
      Effect.gen(function*() {
        const unique = [...new Set(entries.map((entry) => entry.request.query))]
        const values = new Map<string, number>()
        yield* Effect.forEach(
          unique,
          (promql) => fetch(promql).pipe(Effect.map((value) => values.set(promql, value))),
          { concurrency: "unbounded" }
        )
        return entries.map((entry) => values.get(entry.request.query) ?? 0)
      })
  })

/** Turn a resolver into a `query` function usable as the MetricsPort. */
export const queryVia = (resolver: RequestResolver.RequestResolver<PrometheusQuery>) =>
(promql: string): Effect.Effect<number, MetricsUnavailable> =>
  Effect.request(PrometheusQuery({ query: promql }), resolver)

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

      const fetch = (promql: string): Effect.Effect<number, MetricsUnavailable> =>
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

      return { query: queryVia(makeQueryResolver(fetch)) }
    })
  )
