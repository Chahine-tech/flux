import { Duration, Effect, Layer, Schedule, Schema } from "effect"
import { HttpClient, HttpClientRequest } from "effect/unstable/http"
import { RouterPort, RouterUnavailable, type SetTrafficWeightParams, type VersionWeight } from "@flux/application"
import { baseline, redistribute } from "./weights.ts"

/**
 * Caddy routing adapter — implements RouterPort against Caddy's admin API
 * (default `localhost:2019`), the deliberate opposite of the nginx adapter's
 * shape (D20):
 *
 * - **No file, no reload, no lock.** Weights change through a `PATCH` on the
 *   managed route; Caddy applies config changes atomically. The nginx adapter's
 *   Semaphore guards an irreversible write+reload critical section — here there
 *   is none. (Concurrent shifts for the *same* service are already excluded by
 *   admission control.)
 * - **Stateless.** The current weight table is read back from Caddy itself
 *   (`GET /id/…`) before every shift, so a restarted worker loses nothing —
 *   where nginx keeps a `Ref` registry and re-parses its config file.
 * - **Structured state instead of markers.** nginx recovers versions from
 *   `# flux-version=` comments; JSON has no comments, so the version label is
 *   recovered by inverting the address function (`versionOf`, the inverse of
 *   `address`). An upstream the inverse cannot name is reported under its dial
 *   address — visible as drift rather than silently ignored.
 *
 * flux manages one route per service, addressed by Caddy object id
 * `flux-<service>`. Shifts PATCH only `handle/0` (the reverse_proxy handler),
 * so matchers an operator provisioned on the route survive. If the route does
 * not exist it is created (matcher-less) in the configured server's route list.
 * Requires an `HttpClient`.
 */

/** The Caddy object id of the route flux manages for a service. */
export const routeId = (service: string): string => `flux-${service}`

export interface CaddyOptions {
  /** Base URL of the Caddy admin API, e.g. `http://localhost:2019`. */
  readonly adminUrl: string
  /** The `apps.http.servers.<name>` whose routes flux manages. */
  readonly server: string
  /** Resolve the backend address (`host:port`) for a service/version. */
  readonly address: (service: string, version: string) => string
  /** Recover the version from a backend address — the inverse of `address`. */
  readonly versionOf: (service: string, dial: string) => string | undefined
}

// --- Wire schemas (the slice of Caddy's config flux reads) ---

const Upstream = Schema.Struct({ dial: Schema.String })
const SelectionPolicy = Schema.Struct({
  policy: Schema.optionalKey(Schema.String),
  weights: Schema.optionalKey(Schema.Array(Schema.Number))
})
const Handler = Schema.Struct({
  handler: Schema.String,
  upstreams: Schema.optionalKey(Schema.Array(Upstream)),
  load_balancing: Schema.optionalKey(Schema.Struct({
    selection_policy: Schema.optionalKey(SelectionPolicy)
  }))
})
export const CaddyRoute = Schema.Struct({
  handle: Schema.optionalKey(Schema.Array(Handler))
})
export type CaddyRoute = typeof CaddyRoute.Type

/**
 * Build the reverse_proxy handler for a weight table. Zero-weight versions are
 * omitted (Caddy's weighted_round_robin also skips weight-0 upstreams, but a
 * shorter upstream list keeps the config readable). Entries are sorted so the
 * output is deterministic. Weights are index-mapped to upstreams — the contract
 * of `http.reverse_proxy.selection_policies.weighted_round_robin`.
 */
export const renderHandler = (
  service: string,
  versions: Readonly<Record<string, number>>,
  address: CaddyOptions["address"]
): unknown => {
  const entries = Object.entries(versions)
    .filter(([, weight]) => Math.round(weight) > 0)
    .sort(([a], [b]) => a.localeCompare(b))
  return {
    handler: "reverse_proxy",
    upstreams: entries.map(([version]) => ({ dial: address(service, version) })),
    load_balancing: {
      selection_policy: {
        policy: "weighted_round_robin",
        weights: entries.map(([, weight]) => Math.round(weight))
      }
    }
  }
}

/**
 * Read the weight table back from a route (inverse of {@link renderHandler}),
 * normalized to percentages. Missing weights count as 1 each (Caddy's own
 * default when no policy is set), so a hand-provisioned route still reads back.
 */
export const parseRoute = (
  route: CaddyRoute,
  service: string,
  versionOf: CaddyOptions["versionOf"]
): Array<VersionWeight> => {
  const proxy = route.handle?.find((handler) => handler.handler === "reverse_proxy")
  const upstreams = proxy?.upstreams ?? []
  if (proxy === undefined || upstreams.length === 0) {
    return []
  }
  const weights = proxy.load_balancing?.selection_policy?.weights ?? upstreams.map(() => 1)
  const total = weights.reduce((sum, weight) => sum + weight, 0)
  return upstreams.map((upstream, index) => ({
    version: versionOf(service, upstream.dial) ?? upstream.dial,
    weight: total === 0 ? 0 : ((weights[index] ?? 0) / total) * 100
  }))
}

const retryPolicy = {
  schedule: Schedule.exponential(Duration.millis(200)).pipe(Schedule.jittered),
  times: 3
} as const

export const layer = (
  options: CaddyOptions
): Layer.Layer<RouterPort, never, HttpClient.HttpClient> =>
  Layer.effect(
    RouterPort,
    Effect.gen(function*() {
      const client = yield* HttpClient.HttpClient
      const decodeRoute = Schema.decodeUnknownEffect(CaddyRoute)
      const idUrl = (service: string) => `${options.adminUrl}/id/${routeId(service)}`

      const unavailable = (service: string) => (error: unknown): RouterUnavailable =>
        error instanceof RouterUnavailable
          ? error
          : new RouterUnavailable({
            service,
            reason: error instanceof Error ? error.message : String(error)
          })

      /** The route flux manages for `service`, or undefined if none exists yet. */
      const fetchRoute = (service: string): Effect.Effect<CaddyRoute | undefined, RouterUnavailable> =>
        Effect.gen(function*() {
          const response = yield* client.get(idUrl(service))
          if (response.status === 404) {
            return undefined
          }
          if (response.status < 200 || response.status >= 300) {
            return yield* Effect.fail(new RouterUnavailable({ service, reason: `admin API HTTP ${response.status}` }))
          }
          const body = yield* response.json
          return yield* decodeRoute(body)
        }).pipe(
          Effect.retry(retryPolicy),
          Effect.mapError(unavailable(service))
        )

      const expectSuccess = (service: string) => (response: { readonly status: number }) =>
        response.status >= 200 && response.status < 300
          ? Effect.void
          : Effect.fail(new RouterUnavailable({ service, reason: `admin API HTTP ${response.status}` }))

      const setTrafficWeight = (params: SetTrafficWeightParams): Effect.Effect<void, RouterUnavailable> =>
        Effect.gen(function*() {
          const existing = yield* fetchRoute(params.service)
          const current = Object.fromEntries(
            (existing === undefined ? [] : parseRoute(existing, params.service, options.versionOf))
              .map(({ version, weight }) => [version, weight])
          )
          const next = redistribute(baseline(current, params), params.version, params.weight)
          const handler = renderHandler(params.service, next, options.address)

          const request = existing === undefined
            // No route yet → append one (matcher-less) to the managed server.
            ? HttpClientRequest.post(
              `${options.adminUrl}/config/apps/http/servers/${options.server}/routes`
            ).pipe(HttpClientRequest.bodyJsonUnsafe({ "@id": routeId(params.service), handle: [handler] }))
            // Route exists → replace only the handler, keeping operator matchers.
            : HttpClientRequest.patch(`${idUrl(params.service)}/handle/0`).pipe(
              HttpClientRequest.bodyJsonUnsafe(handler)
            )

          yield* client.execute(request).pipe(
            Effect.flatMap(expectSuccess(params.service)),
            Effect.retry(retryPolicy)
          )
        }).pipe(Effect.mapError(unavailable(params.service)))

      const readState = (service: string): Effect.Effect<ReadonlyArray<VersionWeight>, RouterUnavailable> =>
        fetchRoute(service).pipe(
          Effect.map((route) => route === undefined ? [] : parseRoute(route, service, options.versionOf)),
          Effect.mapError(unavailable(service))
        )

      return { setTrafficWeight, readState }
    })
  )
