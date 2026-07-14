import { Effect } from "effect"
import { HealthPort } from "../ports/health.ts"

/**
 * Use case: verify a version is healthy before any traffic is shifted.
 * A pure Effect program against the HealthPort — the Temporal health
 * activity runs this via the worker's ManagedRuntime.
 *
 * `Effect.fn` names the span (`flux.healthCheck`) and captures the call site
 * in stack traces; the dynamic attributes are annotated from the params.
 */
export const healthCheck = Effect.fn("flux.healthCheck")(function*(params: {
  readonly service: string
  readonly version: string
}) {
  yield* Effect.annotateCurrentSpan({ "flux.service": params.service, "flux.version": params.version })
  const health = yield* HealthPort
  yield* health.check(params)
})
