import { Context, type Effect } from "effect"
import type { RouterUnavailable } from "../errors.ts"

/** Route `weight`% of `service` traffic to `version` (0..100). */
export interface SetTrafficWeightParams {
  readonly service: string
  readonly version: string
  readonly weight: number
}

/**
 * Port: shift traffic between versions.
 * Implemented by the nginx/caddy/traefik adapters; mocked in tests.
 */
export class RouterPort extends Context.Service<RouterPort, {
  readonly setTrafficWeight: (params: SetTrafficWeightParams) => Effect.Effect<void, RouterUnavailable>
}>()("RouterPort") {}
