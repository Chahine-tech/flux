import { Context, type Effect } from "effect"
import type { MetricsUnavailable } from "../errors.ts"

/**
 * Port: evaluate a single metric query (PromQL) and return its scalar value.
 *
 * Kept deliberately single-query so the metrics adapter can back it with a
 * RequestResolver: when a monitoring poll evaluates several rules that share a
 * query, the identical requests are deduplicated into one backend fetch.
 */
export class MetricsPort extends Context.Service<MetricsPort, {
  readonly query: (promql: string) => Effect.Effect<number, MetricsUnavailable>
}>()("MetricsPort") {}
