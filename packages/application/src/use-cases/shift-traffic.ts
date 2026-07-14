import { Effect } from "effect"
import { RouterPort, type SetTrafficWeightParams } from "../ports/router.ts"

/**
 * Use case: route a share of a service's traffic to a version.
 * Thin program against the RouterPort — driven by the traffic-shift activity.
 */
export const shiftTraffic = Effect.fn("flux.shiftTraffic")(function*(params: SetTrafficWeightParams) {
  yield* Effect.annotateCurrentSpan({
    "flux.service": params.service,
    "flux.version": params.version,
    "flux.weight": params.weight
  })
  const router = yield* RouterPort
  yield* router.setTrafficWeight(params)
})
