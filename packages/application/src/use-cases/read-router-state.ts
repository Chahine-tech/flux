import { Effect } from "effect"
import { RouterPort } from "../ports/router.ts"

/**
 * Read the routing actually in effect for a service — the "actual"
 * side of a drift check. A thin use case over the port so the activity that
 * exposes it to a workflow stays uniform with the others.
 */
export const readRouterState = Effect.fn("flux.readRouterState")(function*(service: string) {
  yield* Effect.annotateCurrentSpan({ "flux.service": service })
  const router = yield* RouterPort
  return yield* router.readState(service)
})
