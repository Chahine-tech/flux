import { Schema } from "effect"
import { Identifier, Thresholds } from "@flux/domain"

/**
 * The body of `POST /deployments` — the request that starts a canary.
 *
 * Durations cross as milliseconds (plain numbers), matching the Effect-free
 * `DeploymentInput` the workflow consumes (D6): the control plane decodes this
 * request and passes the value straight to `client.workflow.start`. `rules`
 * reuses the domain `Thresholds` schema so the failure budget has one definition.
 */

const NonEmptyString = Schema.String.check(Schema.isMinLength(1))
const NonNegative = Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0))

/** One canary stage: shift `percent` of traffic, then monitor for `monitorMs`. */
export const DeploymentStep = Schema.Struct({
  percent: Schema.Finite.check(Schema.isBetween({ minimum: 0, maximum: 100 })),
  monitorMs: NonNegative,
  requiresApproval: Schema.Boolean,
  approvalTimeoutMs: Schema.optionalKey(NonNegative)
})
export type DeploymentStep = typeof DeploymentStep.Type

export const TriggerDeploymentRequest = Schema.Struct({
  // `Identifier`, not just non-empty: these are interpolated into nginx config
  // and PromQL by the adapters, so the charset is locked down at the boundary.
  service: Identifier,
  version: Identifier,
  previousVersion: Identifier,
  steps: Schema.NonEmptyArray(DeploymentStep),
  rules: Thresholds,
  pollIntervalMs: Schema.Finite.check(Schema.isGreaterThan(0))
})
export type TriggerDeploymentRequest = typeof TriggerDeploymentRequest.Type

/** `POST /deployments` returns the started workflow's id. */
export const TriggerDeploymentResponse = Schema.Struct({
  workflowId: NonEmptyString
})
export type TriggerDeploymentResponse = typeof TriggerDeploymentResponse.Type

/**
 * The body of `POST /deployments/multi` — roll one version out across several
 * services at once, as a parent workflow over one child per service (N4/D13).
 */
export const TriggerMultiRequest = Schema.Struct({
  services: Schema.NonEmptyArray(TriggerDeploymentRequest),
  /** How many services roll out concurrently. */
  maxConcurrency: Schema.Finite.check(Schema.isGreaterThanOrEqualTo(1)),
  /** If true, the first non-success aborts every in-flight sibling. */
  failFast: Schema.Boolean
})
export type TriggerMultiRequest = typeof TriggerMultiRequest.Type
