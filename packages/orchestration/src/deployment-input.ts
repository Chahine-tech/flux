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

/** A metric rule crossing to the workflow: watch `query`, breach if `> max`. */
export interface DeploymentRule {
  readonly name: string
  readonly query: string
  readonly max: number
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
  readonly rules: ReadonlyArray<DeploymentRule>
  /** How often each step samples metrics while monitoring, in milliseconds. */
  readonly pollIntervalMs: number
}

/** Custom Temporal search-attribute names — powers `flux history` visibility queries. */
export const SEARCH_ATTRIBUTES = {
  service: "FluxService",
  version: "FluxVersion",
  status: "FluxStatus"
} as const

/** Live deployment state, exposed by the workflow's `status` query. */
export interface DeploymentState {
  readonly phase:
    | "health-checking"
    | "shifting"
    | "monitoring"
    | "awaiting-approval"
    | "rolling-back"
    | "done"
  readonly service: string
  readonly version: string
  readonly currentPercent: number
  readonly stepIndex: number
  readonly totalSteps: number
  readonly outcome?: DeploymentResult["kind"]
}

export type DeploymentResult =
  | { readonly kind: "Succeeded"; readonly service: string; readonly version: string }
  | {
    readonly kind: "RolledBack"
    readonly service: string
    readonly toVersion: string
    readonly atPercent: number
    readonly breaches: ReadonlyArray<{
      readonly metric: string
      readonly observed: number
      readonly limit: number
    }>
  }
  | { readonly kind: "Aborted"; readonly service: string; readonly atPercent: number }
  | { readonly kind: "Failed"; readonly service: string; readonly reason: string }
