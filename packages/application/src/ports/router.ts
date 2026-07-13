import { Context, type Effect } from "effect"
import type { RouterUnavailable } from "../errors.ts"

/** Route `weight`% of `service` traffic to `version` (0..100). */
export interface SetTrafficWeightParams {
  readonly service: string
  readonly version: string
  readonly weight: number
}

/** A version and the traffic weight it currently receives. */
export interface VersionWeight {
  readonly version: string
  readonly weight: number
}

/**
 * Port: shift traffic between versions and read back the actual routing.
 * Implemented by the nginx/caddy/traefik adapters; mocked in tests.
 */
export class RouterPort extends Context.Service<RouterPort, {
  readonly setTrafficWeight: (params: SetTrafficWeightParams) => Effect.Effect<void, RouterUnavailable>
  /**
   * The routing actually in effect for `service` — read from the live backend
   * (e.g. the on-disk nginx config), not from what flux believes it applied.
   * This is what drift detection (N4/D17) compares against the desired state.
   */
  readonly readState: (service: string) => Effect.Effect<ReadonlyArray<VersionWeight>, RouterUnavailable>
}>()("RouterPort") {}
