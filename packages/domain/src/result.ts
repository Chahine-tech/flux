import { Schema } from "effect"
import { MetricsSnapshot } from "./metrics.ts"

/**
 * Terminal outcome of a deployment workflow, as a discriminated union.
 * Modelled with `Schema.TaggedStruct` members so it is both a valid Temporal
 * payload and exhaustively matchable (`Match`) at the CLI / notifier.
 */

/** Fully rolled out to 100%. */
export const Succeeded = Schema.TaggedStruct("Succeeded", {
  service: Schema.String,
  version: Schema.String
})

/** Rolled back to the previous version after a threshold breach. */
export const RolledBack = Schema.TaggedStruct("RolledBack", {
  service: Schema.String,
  fromVersion: Schema.String,
  toVersion: Schema.String,
  atPercent: Schema.Finite,
  metrics: MetricsSnapshot
})

/** Manually aborted via signal/update before completing. */
export const Aborted = Schema.TaggedStruct("Aborted", {
  service: Schema.String,
  atPercent: Schema.Finite
})

export const DeploymentOutcome = Schema.Union([Succeeded, RolledBack, Aborted])
export type DeploymentOutcome = typeof DeploymentOutcome.Type
