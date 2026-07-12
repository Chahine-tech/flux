import { Effect } from "effect"
import type { HealthCheckFailed } from "@flux/domain"
import { HealthPort } from "../ports/health.ts"

/**
 * Use case: verify a version is healthy before any traffic is shifted.
 * A pure Effect program against the HealthPort — the Temporal health
 * activity runs this via the worker's ManagedRuntime.
 */
export const healthCheck = (params: {
  readonly service: string
  readonly version: string
}): Effect.Effect<void, HealthCheckFailed, HealthPort> =>
  Effect.gen(function*() {
    const health = yield* HealthPort
    yield* health.check(params)
  })
