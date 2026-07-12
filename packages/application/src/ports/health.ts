import { Context, type Effect } from "effect"
import type { HealthCheckFailed } from "@flux/domain"

/** Probe the health of a specific version before shifting traffic to it. */
export interface HealthCheckParams {
  readonly service: string
  readonly version: string
}

/**
 * Port: verify a version is healthy. Fails with the domain error
 * `HealthCheckFailed` (a business-meaningful outcome, not infrastructure).
 */
export class HealthPort extends Context.Service<HealthPort, {
  readonly check: (params: HealthCheckParams) => Effect.Effect<void, HealthCheckFailed>
}>()("HealthPort") {}
