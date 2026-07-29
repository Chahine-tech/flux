import type { Notification } from "@flux/application"
import type { ThresholdEvaluation } from "@flux/domain"
import type { DeploymentRule } from "../deployment-input.ts"

/**
 * The activity contract shared between the workflow (which `proxyActivities`
 * against it) and the activity implementations. Type-only imports keep this
 * file — and therefore the workflow bundle — free of the Effect runtime.
 *
 * `monitorStep` returns the plain, tagged `ThresholdEvaluation` so the workflow
 * can branch on `_tag` without ever importing the (Schema-backed, Effect-side)
 * `evaluateThresholds` rule.
 */
export interface DeploymentActivities {
  healthCheck(params: { readonly service: string; readonly version: string }): Promise<void>

  setTrafficWeight(params: {
    readonly service: string
    readonly version: string
    readonly weight: number
    /** Seeds an adapter that has no state for the service yet (see RouterPort). */
    readonly previousVersion?: string
  }): Promise<void>

  monitorStep(params: {
    readonly service: string
    readonly version: string
    readonly windowMs: number
    readonly pollIntervalMs: number
    readonly rules: ReadonlyArray<DeploymentRule>
  }): Promise<ThresholdEvaluation>

  notify(params: {
    readonly kind: Notification["kind"]
    readonly service: string
    readonly message: string
  }): Promise<void>

  /** Read the routing actually in effect for a service (drift detection). */
  readRouterState(params: { readonly service: string }): Promise<ReadonlyArray<{ readonly version: string; readonly weight: number }>>

  /** Record the terminal outcome of a deployment for self-instrumentation. */
  recordOutcome(outcome: string): Promise<void>

  /**
   * Draft an LLM rollback postmortem. Best-effort: it logs the analysis
   * (correlated to the deployment) and never rejects, so a missing API key or a
   * provider hiccup can't disturb a rollback that has already completed.
   */
  postmortem(params: {
    readonly service: string
    readonly version: string
    readonly previousVersion: string
    readonly atPercent: number
    readonly breaches: ReadonlyArray<{ readonly metric: string; readonly observed: number; readonly limit: number }>
  }): Promise<void>
}
