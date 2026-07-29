import { Schema } from "effect"
import { ThresholdBreach } from "./thresholds.ts"

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
  breaches: Schema.Array(ThresholdBreach)
})

/** Manually aborted via signal/update before completing. */
export const Aborted = Schema.TaggedStruct("Aborted", {
  service: Schema.String,
  atPercent: Schema.Finite
})

// This union is the three "clean" business endings. Operational failures —
// `Failed` (an activity failed) and `RollbackFailed` (D31, the rollback did not
// restore health) — are not business outcomes; they live workflow-side on
// `DeploymentResult` and the `ResultKind` contract. (`RollbackFailed` is also a
// typed *error* in `errors.ts` — the activity-level failure — a distinct concept
// from the terminal result kind.)
export const DeploymentOutcome = Schema.Union([Succeeded, RolledBack, Aborted])
export type DeploymentOutcome = typeof DeploymentOutcome.Type
