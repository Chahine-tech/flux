import { HttpClient, HttpClientRequest } from "effect/unstable/http"
import { HttpApiClient } from "effect/unstable/httpapi"
import { NodeHttpClient } from "@effect/platform-node"
import { FluxApi } from "@flux/contracts"

/**
 * A typed client for the control plane, derived from the shared `FluxApi`
 * contract (N4). Deployments start through here — `POST /deployments` — so they
 * pass the admission controller, instead of the CLI starting the workflow
 * directly on Temporal. Reads (`status`, `history`) stay on the direct Temporal
 * path in `temporal.ts`.
 *
 * When the control plane is configured with `API_TOKEN`, set `FLUX_TOKEN` to the
 * same value — every request then carries the bearer token the contract's
 * `Authorization` middleware expects.
 */
export const makeClient = (baseUrl: string) => {
  const token = process.env.FLUX_TOKEN
  return HttpApiClient.make(FluxApi, {
    baseUrl,
    ...(token
      ? {
        transformClient: HttpClient.mapRequest(
          HttpClientRequest.setHeader("authorization", `Bearer ${token}`)
        )
      }
      : {})
  })
}

/** Provides the HTTP client the derived API client needs. */
export const clientLayer = NodeHttpClient.layerUndici
