import { Context, type Duration, type Effect } from "effect"
import type { MetricsSnapshot } from "@flux/domain"
import type { MetricsUnavailable } from "../errors.ts"

/** Parameters for a single metrics collection over a monitoring window. */
export interface CollectParams {
  readonly service: string
  readonly version: string
  readonly window: Duration.Duration
}

/**
 * Port: read the metrics flux watches during a canary step.
 * Implemented by the Prometheus adapter (@flux/adapters); mocked in tests.
 */
export class MetricsPort extends Context.Service<MetricsPort, {
  readonly collect: (params: CollectParams) => Effect.Effect<MetricsSnapshot, MetricsUnavailable>
}>()("MetricsPort") {}
