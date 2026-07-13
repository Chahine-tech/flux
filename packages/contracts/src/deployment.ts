import { Schema } from "effect"

/**
 * Wire schemas for a deployment's observable state, shared by the control plane
 * (which serves them over HTTP and RPC) and the CLI (which decodes them).
 *
 * These mirror the Effect-free interfaces the workflow exposes (`@flux/orchestration`
 * `DeploymentState` / `DeploymentResult`) — the workflow bundle must stay free of
 * Effect (D6), so the runtime-validated versions live here, on the Effect side of
 * the boundary. The two representations are kept structurally compatible on purpose.
 */

const NonEmptyString = Schema.String.check(Schema.isMinLength(1))

/** The phase a canary is currently in — matches the workflow's `status` query. */
export const DeploymentPhase = Schema.Literals([
  "health-checking",
  "shifting",
  "monitoring",
  "awaiting-approval",
  "rolling-back",
  "done"
])
export type DeploymentPhase = typeof DeploymentPhase.Type

/** The terminal outcome kinds — used both as `outcome` and as a history filter. */
export const ResultKind = Schema.Literals(["Succeeded", "RolledBack", "Aborted", "Failed"])
export type ResultKind = typeof ResultKind.Type

/** Live canary state, exposed by the workflow `status` query and streamed on `watch`. */
export const DeploymentState = Schema.Struct({
  phase: DeploymentPhase,
  service: NonEmptyString,
  version: NonEmptyString,
  currentPercent: Schema.Finite.check(Schema.isBetween({ minimum: 0, maximum: 100 })),
  stepIndex: Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0)),
  totalSteps: Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0)),
  outcome: Schema.optionalKey(ResultKind)
})
export type DeploymentState = typeof DeploymentState.Type

/** One row in `flux history` — projected from Temporal advanced visibility. */
export const DeploymentSummary = Schema.Struct({
  workflowId: NonEmptyString,
  status: Schema.String,
  startTime: Schema.String
})
export type DeploymentSummary = typeof DeploymentSummary.Type
