import type { Notification } from "@flux/application"
import type { ThresholdEvaluation } from "@flux/domain"
import type { DeploymentThresholds } from "../deployment-input.ts"

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
  }): Promise<void>

  monitorStep(params: {
    readonly service: string
    readonly version: string
    readonly windowMs: number
    readonly pollIntervalMs: number
    readonly thresholds: DeploymentThresholds
  }): Promise<ThresholdEvaluation>

  notify(params: {
    readonly kind: Notification["kind"]
    readonly service: string
    readonly message: string
  }): Promise<void>
}
