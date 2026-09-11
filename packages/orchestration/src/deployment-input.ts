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

/**
 * The rollout strategy the workflow runs. A discriminated union so the
 * workflow branches on `kind`: `canary` shifts traffic gradually across steps;
 * `blue-green` flips 100% at once after a health check and bakes.
 */
export type DeploymentStrategy =
  | { readonly kind: "canary"; readonly steps: ReadonlyArray<DeploymentStepInput> }
  | {
    readonly kind: "blue-green"
    readonly bakeMs: number
    readonly requiresApproval: boolean
    readonly approvalTimeoutMs?: number
  }

export interface DeploymentInput {
  readonly service: string
  readonly version: string
  readonly previousVersion: string
  /**
   * The rollout strategy. Optional so two things keep working: histories
   * recorded before strategy support (which carry a top-level `steps` array and no
   * `strategy`), and callers that pass `steps` as a canary shorthand. When
   * absent, the workflow normalizes to `{ kind: "canary", steps }` — an identical
   * command sequence, which is what keeps the committed histories replaying.
   */
  readonly strategy?: DeploymentStrategy
  /** Canary steps as a top-level shorthand / back-compat — the `strategy` fallback. */
  readonly steps?: ReadonlyArray<DeploymentStepInput>
  readonly rules: ReadonlyArray<DeploymentRule>
  /** How often each step samples metrics while monitoring, in milliseconds. */
  readonly pollIntervalMs: number
  /**
   * Bound a single workflow run to this many steps: after completing that many,
   * the workflow continues-as-new with the remaining steps to keep history
   * small. Temporal's own `continueAsNewSuggested` triggers the same
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
 * A compiled rollout plan. Structurally identical to `RolloutPlan` in
 * `@flux/domain`, restated here because this file imports nothing at all — the
 * workflow bundle must not reach the Effect runtime even through a type-only
 * path. `mapper.ts`, on the Effect side, holds a compile-time assertion that
 * the two shapes still match, so drift is a type error rather than a comment.
 *
 * The graph that produced this never crosses: it is compiled before the
 * workflow starts, so the plan is frozen in the start event and replays
 * identically even if the dependency config is later edited.
 */
export interface RolloutPlanInput {
  /** Every service in topological order: a dependency always precedes its dependents. */
  readonly order: ReadonlyArray<string>
  /** Direct dependencies per service, normalized so every service has an entry. */
  readonly dependsOn: Readonly<Record<string, ReadonlyArray<string>>>
  /** Everything downstream of a service, for skipping in one lookup. */
  readonly transitiveDependents: Readonly<Record<string, ReadonlyArray<string>>>
  /** Topological levels. Display only — the workflow schedules from `dependsOn`. */
  readonly waves: ReadonlyArray<ReadonlyArray<string>>
}

/**
 * What a non-success does to the rest of the rollout.
 *
 * - `fail-fast` aborts every in-flight sibling (the original behaviour).
 * - `abort-dependents` only stops what transitively depends on the failure;
 *   independent branches run to completion. Needs a dependency graph to mean
 *   anything, which is why it arrived with one.
 * - `continue` lets everything that still can, run.
 */
export type RolloutFailurePolicy = "fail-fast" | "abort-dependents" | "continue"

/**
 * A service that never started because something upstream did not succeed.
 * Deliberately *not* a `DeploymentResult`: no child workflow ever ran, so
 * reporting `Failed` would claim a rollout that never happened.
 */
export interface SkippedDeployment {
  readonly kind: "Skipped"
  readonly service: string
  /** The upstream service whose outcome blocked this one. */
  readonly blockedBy: string
}

/** What a multi-service rollout can report per service. */
export type MultiServiceOutcome = DeploymentResult | SkippedDeployment

/**
 * Roll out a version across several services at once. Modelled as a
 * parent workflow over N per-service `deploymentWorkflow` children.
 */
export interface MultiServiceInput {
  readonly services: ReadonlyArray<DeploymentInput>
  /** Maximum number of services rolling out concurrently. */
  readonly maxConcurrency: number
  /**
   * If true, the first non-success aborts every in-flight sibling.
   * Superseded by `onFailure`; kept because histories recorded before the
   * policy existed carry only this, and must keep replaying.
   */
  readonly failFast: boolean
  /** When absent, derived from `failFast` so older inputs behave identically. */
  readonly onFailure?: RolloutFailurePolicy
  /**
   * When absent, every service is treated as independent — which is exactly
   * what the rollout did before dependencies existed, so old histories replay
   * down the same path.
   */
  readonly plan?: RolloutPlanInput
}

/** Aggregate outcome of a multi-service rollout. */
export interface MultiServiceResult {
  readonly kind: "AllSucceeded" | "SomeFailed"
  readonly perService: ReadonlyArray<{ readonly service: string; readonly result: MultiServiceOutcome }>
}

/** Live aggregate state, exposed by the parent's `status` query. */
export interface MultiServiceState {
  readonly total: number
  readonly running: number
  readonly succeeded: number
  readonly failed: number
  /** Never started, because something they depend on did not succeed. */
  readonly skipped: number
}

/** A version and the traffic weight it should receive. */
export interface RouteWeight {
  readonly version: string
  readonly weight: number
}

/** Input to a drift check: does the router actually route as desired? */
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
  | {
    readonly kind: "RollbackFailed"
    readonly service: string
    readonly version: string
    readonly toVersion: string
    readonly atPercent: number
    readonly reason: string
  }
