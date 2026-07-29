import { Duration, Effect, Layer, Redacted, Request, RequestResolver, Schedule } from "effect"
import { HttpClient, HttpClientResponse } from "effect/unstable/http"
import { MetricsPort, MetricsUnavailable } from "@flux/application"

/**
 * A MetricsPort backed by a plain JSON endpoint, for apps that don't run
 * Prometheus. Needs an HttpClient.
 *
 * MetricsPort.query is just a string, and each adapter is free to read it how it
 * likes. The Prometheus adapter treats it as PromQL. This one treats it as
 * "<url> <json-path>": GET the url, pull a number out at a dotted path like
 * data.checkout.error_rate (numbers in the path index into arrays). Same rules,
 * different reading of the query.
 *
 * Same RequestResolver as the Prometheus adapter, so two rules with the same
 * "<url> <path>" hit the endpoint once per poll. Transient HTTP errors retry
 * with backoff here, not in the workflow.
 */

/** Build a metrics query string for this adapter. */
export const query = (url: string, path: string): string => `${url} ${path}`

/** Split a query into its URL and JSON path, or `undefined` if malformed. */
export const parseQuery = (q: string): { readonly url: string; readonly path: string } | undefined => {
  const idx = q.search(/\s/)
  if (idx === -1) return undefined
  const url = q.slice(0, idx)
  const path = q.slice(idx + 1).trim()
  return url === "" || path === "" ? undefined : { url, path }
}

/** Walk a dotted path into decoded JSON; numeric segments index arrays. */
export const readAtPath = (json: unknown, path: string): unknown => {
  let current: unknown = json
  for (const segment of path.split(".")) {
    if (current === null || current === undefined) return undefined
    if (Array.isArray(current)) {
      const index = Number(segment)
      if (!Number.isInteger(index)) return undefined
      current = current[index]
    } else if (typeof current === "object") {
      current = (current as Record<string, unknown>)[segment]
    } else {
      return undefined
    }
  }
  return current
}

/** Coerce a JSON value to a finite number, or `undefined` if it isn't one. */
export const toNumber = (value: unknown): number | undefined => {
  if (typeof value === "number") return Number.isFinite(value) ? value : undefined
  if (typeof value === "string") {
    const parsed = Number(value)
    return Number.isFinite(parsed) ? parsed : undefined
  }
  return undefined
}

interface HttpJsonQuery extends Request.Request<number, MetricsUnavailable> {
  readonly _tag: "HttpJsonQuery"
  readonly query: string
}
const HttpJsonQuery = Request.tagged<HttpJsonQuery>("HttpJsonQuery")

/** Resolver that fetches each distinct `"<url> <path>"` once per batch. */
export const makeQueryResolver = (
  fetch: (query: string) => Effect.Effect<number, MetricsUnavailable>
): RequestResolver.RequestResolver<HttpJsonQuery> =>
  RequestResolver.fromEffectTagged<HttpJsonQuery>()({
    HttpJsonQuery: (entries) =>
      Effect.gen(function*() {
        const unique = [...new Set(entries.map((entry) => entry.request.query))]
        const values = new Map<string, number>()
        yield* Effect.forEach(
          unique,
          (query) => fetch(query).pipe(Effect.map((value) => values.set(query, value))),
          { concurrency: "unbounded" }
        )
        return entries.map((entry) => values.get(entry.request.query) ?? 0)
      })
  })

/** Turn a resolver into a `query` function usable as the MetricsPort. */
export const queryVia = (resolver: RequestResolver.RequestResolver<HttpJsonQuery>) =>
(query: string): Effect.Effect<number, MetricsUnavailable> =>
  Effect.request(HttpJsonQuery({ query }), resolver)

export interface HttpJsonOptions {
  /** Optional bearer token for authenticated endpoints; empty (default) sends no auth. */
  readonly authToken?: Redacted.Redacted<string>
}

const retryPolicy = {
  schedule: Schedule.exponential(Duration.millis(200)).pipe(Schedule.jittered),
  times: 3
} as const

export const layer = (
  options: HttpJsonOptions = {}
): Layer.Layer<MetricsPort, never, HttpClient.HttpClient> =>
  Layer.effect(
    MetricsPort,
    Effect.gen(function*() {
      const client = yield* HttpClient.HttpClient
      const token = options.authToken === undefined ? "" : Redacted.value(options.authToken)
      const unavailable = (reason: string) => new MetricsUnavailable({ service: "http-json", reason })

      const fetch = (q: string): Effect.Effect<number, MetricsUnavailable> => {
        const parsed = parseQuery(q)
        if (parsed === undefined) {
          return Effect.fail(unavailable(`invalid query "${q}" — expected "<url> <json-path>"`))
        }
        const request = token === ""
          ? client.get(parsed.url)
          : client.get(parsed.url, { headers: { authorization: `Bearer ${token}` } })

        // Retry covers only transport/decode; a missing path is a config error,
        // not a transient one, so it fails after the retried fetch resolves.
        return request.pipe(
          Effect.flatMap(HttpClientResponse.filterStatusOk),
          Effect.flatMap((response) => response.json),
          Effect.retry(retryPolicy),
          Effect.mapError((cause) => unavailable(cause instanceof Error ? cause.message : String(cause))),
          Effect.flatMap((json) => {
            const value = toNumber(readAtPath(json, parsed.path))
            return value === undefined
              ? Effect.fail(unavailable(`no numeric value at "${parsed.path}" from ${parsed.url}`))
              : Effect.succeed(value)
          })
        )
      }

      return { query: queryVia(makeQueryResolver(fetch)) }
    })
  )
