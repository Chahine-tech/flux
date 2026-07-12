import { Effect } from "effect"
import type { RouterUnavailable } from "../errors.ts"
import { RouterPort, type SetTrafficWeightParams } from "../ports/router.ts"

/**
 * Use case: route a share of a service's traffic to a version.
 * Thin program against the RouterPort — driven by the traffic-shift activity.
 */
export const shiftTraffic = (
  params: SetTrafficWeightParams
): Effect.Effect<void, RouterUnavailable, RouterPort> =>
  Effect.gen(function*() {
    const router = yield* RouterPort
    yield* router.setTrafficWeight(params)
  })
