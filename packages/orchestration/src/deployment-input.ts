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
  /**
   * Bound a single workflow run to this many steps: after completing that many,
   * the workflow continues-as-new with the remaining steps to keep history
   * small (N4/D16). Temporal's own `continueAsNewSuggested` triggers the same
   * behaviour automatically for long histories; this is the explicit override.
   */
  readonly continueAsNewAfterSteps?: number
  /**
   * Internal: set by the workflow when it continues-as-new mid-rollout, so the
   * next run resumes correctly rather than re-running from the top.
   */
  readonly resumeFrom?: {
    /** Steps already completed in earlier runs — for `stepIndex`/`totalSteps`. */
    readonly completedSteps: number
    /** Traffic already diverted to the new version — rebuild the rollback compensation. */
    readonly trafficShifted: boolean
    /** Percent reached before this run — the rollback point if aborted immediately. */
    readonly lastPercent: number
  }
}

/**
 * Roll out a version across several services at once (N4/D13). Modelled as a
 * parent workflow over N per-service `deploymentWorkflow` children.
 */
export interface MultiServiceInput {
  readonly services: ReadonlyArray<DeploymentInput>
  /** Maximum number of services rolling out concurrently. */
  readonly maxConcurrency: number
  /** If true, the first non-success aborts every in-flight sibling. */
  readonly failFast: boolean
}

/** Aggregate outcome of a multi-service rollout. */
export interface MultiServiceResult {
  readonly kind: "AllSucceeded" | "SomeFailed"
  readonly perService: ReadonlyArray<{ readonly service: string; readonly result: DeploymentResult }>
}

/** Live aggregate state, exposed by the parent's `status` query. */
export interface MultiServiceState {
  readonly total: number
  readonly running: number
  readonly succeeded: number
  readonly failed: number
}

/** A version and the traffic weight it should receive. */
export interface RouteWeight {
  readonly version: string
  readonly weight: number
}

/** Input to a drift check (N4/D17): does the router actually route as desired? */
export interface DriftCheckInput {
  readonly service: string
  readonly desired: ReadonlyArray<RouteWeight>
  /** If true, a detected drift is reconciled by re-applying the desired weights. */
  readonly reconcile: boolean
}

/** Outcome of a drift check. */
export interface DriftReport {
  readonly service: string
  readonly drifted: boolean
  readonly reconciled: boolean
  readonly desired: ReadonlyArray<RouteWeight>
  readonly actual: ReadonlyArray<RouteWeight>
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
