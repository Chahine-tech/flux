import { HttpApiClient } from "effect/unstable/httpapi"
import { NodeHttpClient } from "@effect/platform-node"
import { FluxApi } from "@flux/contracts"

/**
 * A typed client for the control plane, derived from the shared `FluxApi`
 * contract (N4). Deployments start through here — `POST /deployments` — so they
 * pass the admission controller, instead of the CLI starting the workflow
 * directly on Temporal. Reads (`status`, `history`) stay on the direct Temporal
 * path in `temporal.ts`.
 */
export const makeClient = (baseUrl: string) => HttpApiClient.make(FluxApi, { baseUrl })

/** Provides the HTTP client the derived API client needs. */
export const clientLayer = NodeHttpClient.layerUndici
