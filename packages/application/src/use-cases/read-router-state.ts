import { Effect } from "effect"
import type { RouterUnavailable } from "../errors.ts"
import { RouterPort, type VersionWeight } from "../ports/router.ts"

/**
 * Read the routing actually in effect for a service (N4/D17) — the "actual"
 * side of a drift check. A thin use case over the port so the activity that
 * exposes it to a workflow stays uniform with the others.
 */
export const readRouterState = (
  service: string
): Effect.Effect<ReadonlyArray<VersionWeight>, RouterUnavailable, RouterPort> =>
  Effect.flatMap(RouterPort, (router) => router.readState(service))
