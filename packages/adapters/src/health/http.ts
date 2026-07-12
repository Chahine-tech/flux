import { Effect, Layer } from "effect"
import { HttpClient } from "effect/unstable/http"
import { type HealthCheckParams, HealthPort } from "@flux/application"
import { HealthCheckFailed } from "@flux/domain"

/**
 * HTTP health-check adapter — implements HealthPort by probing a URL and
 * treating any 2xx as healthy. Both non-2xx responses and transport errors
 * become the domain error `HealthCheckFailed`. Requires an `HttpClient`.
 */

/** A 2xx response counts as healthy. */
export const isHealthyStatus = (status: number): boolean => status >= 200 && status < 300

export interface HttpHealthOptions {
  /** Resolve the health-probe URL for a given service/version. */
  readonly url: (params: HealthCheckParams) => string
}

export const layer = (
  options: HttpHealthOptions
): Layer.Layer<HealthPort, never, HttpClient.HttpClient> =>
  Layer.effect(
    HealthPort,
    Effect.gen(function*() {
      const client = yield* HttpClient.HttpClient

      return {
        check: (params: HealthCheckParams): Effect.Effect<void, HealthCheckFailed> =>
          client.get(options.url(params)).pipe(
            Effect.flatMap((response) =>
              isHealthyStatus(response.status)
                ? Effect.void
                : Effect.fail(
                  new HealthCheckFailed({
                    service: params.service,
                    version: params.version,
                    reason: `unhealthy HTTP ${response.status}`
                  })
                )
            ),
            Effect.mapError((error) =>
              error instanceof HealthCheckFailed
                ? error
                : new HealthCheckFailed({
                  service: params.service,
                  version: params.version,
                  reason: error instanceof Error ? error.message : String(error)
                })
            )
          )
      }
    })
  )
