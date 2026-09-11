import { Schema } from "effect"
import { DeploymentWindow, Identifier, Thresholds } from "@flux/domain"

/**
 * The body of `POST /deployments` — the request that starts a canary.
 *
 * Durations cross as milliseconds (plain numbers), matching the Effect-free
 * `DeploymentInput` the workflow consumes: the control plane decodes this
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

/**
 * The rollout strategy on the wire, a discriminated union on `kind` that
 * matches the workflow's `DeploymentStrategy`: `canary` carries its steps;
 * `blue-green` carries a bake window and an optional approval.
 */
export const StrategyInput = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("canary"),
    steps: Schema.NonEmptyArray(DeploymentStep)
  }),
  Schema.Struct({
    kind: Schema.Literal("blue-green"),
    bakeMs: NonNegative,
    requiresApproval: Schema.Boolean,
    approvalTimeoutMs: Schema.optionalKey(NonNegative)
  })
])
export type StrategyInput = typeof StrategyInput.Type

export const TriggerDeploymentRequest = Schema.Struct({
  // `Identifier`, not just non-empty: these are interpolated into nginx config
  // and PromQL by the adapters, so the charset is locked down at the boundary.
  service: Identifier,
  version: Identifier,
  previousVersion: Identifier,
  strategy: StrategyInput,
  rules: Thresholds,
  pollIntervalMs: Schema.Finite.check(Schema.isGreaterThan(0)),
  /**
   * Optional deploy window as a cron expression. The canary may only
   * start while `now` is inside it; absent means always allowed. Checked by the
   * control plane before admission — it never reaches the workflow.
   */
  window: Schema.optionalKey(DeploymentWindow)
})
export type TriggerDeploymentRequest = typeof TriggerDeploymentRequest.Type

/** `POST /deployments` returns the started workflow's id. */
export const TriggerDeploymentResponse = Schema.Struct({
  workflowId: NonEmptyString
})
export type TriggerDeploymentResponse = typeof TriggerDeploymentResponse.Type

/**
 * The body of `POST /deployments/multi` — roll one version out across several
 * services at once, as a parent workflow over one child per service.
 */
export const TriggerMultiRequest = Schema.Struct({
  services: Schema.NonEmptyArray(TriggerDeploymentRequest),
  /** How many services roll out concurrently. */
  maxConcurrency: Schema.Finite.check(Schema.isGreaterThanOrEqualTo(1)),
  /** If true, the first non-success aborts every in-flight sibling. */
  failFast: Schema.Boolean,
  /**
   * `{ dependent: [what it needs first] }`. The control plane compiles this
   * into a topological plan before starting the workflow, so a cycle is a 422
   * here rather than a deadlock later. Absent means every service is
   * independent, which is how the rollout behaved before dependencies existed.
   */
  dependsOn: Schema.optionalKey(Schema.Record(Identifier, Schema.Array(Identifier))),
  /**
   * What a non-success does to the rest. Absent falls back to `failFast`, so
   * existing callers keep their behaviour without sending this.
   */
  onFailure: Schema.optionalKey(Schema.Literals(["fail-fast", "abort-dependents", "continue"]))
})
export type TriggerMultiRequest = typeof TriggerMultiRequest.Type
