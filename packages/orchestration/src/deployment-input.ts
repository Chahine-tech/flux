/**
 * Workflow-facing types — deliberately Effect-free and fully serializable.
 *
 * The workflow bundle must stay free of the Effect runtime,
 * so durations cross this boundary as **milliseconds** (plain numbers), not
 * Effect `Duration` objects. The `configToInput` mapper (Effect side) converts
 * a domain `DeploymentConfig` into this shape before a workflow is started.
 *
 * This file must never import `effect` (directly or transitively).
 */

export interface DeploymentThresholds {
  readonly maxErrorRate: number
  readonly maxP99LatencyMs: number
}

export interface DeploymentStepInput {
  readonly percent: number
  readonly monitorMs: number
  readonly requiresApproval: boolean
  readonly approvalTimeoutMs?: number
}

export interface DeploymentInput {
  readonly service: string
  readonly version: string
  readonly previousVersion: string
  readonly steps: ReadonlyArray<DeploymentStepInput>
  readonly thresholds: DeploymentThresholds
}

export type DeploymentResult =
  | { readonly kind: "Succeeded"; readonly service: string; readonly version: string }
  | {
    readonly kind: "RolledBack"
    readonly service: string
    readonly toVersion: string
    readonly atPercent: number
  }
  | { readonly kind: "Aborted"; readonly service: string; readonly atPercent: number }
  | { readonly kind: "Failed"; readonly service: string; readonly reason: string }
