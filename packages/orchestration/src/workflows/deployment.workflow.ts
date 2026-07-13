import {
  ActivityFailure,
  ApplicationFailure,
  CancellationScope,
  condition,
  continueAsNew,
  defineQuery,
  defineSignal,
  defineUpdate,
  isCancellation,
  log,
  proxyActivities,
  proxyLocalActivities,
  setHandler,
  setWorkflowOptions,
  upsertSearchAttributes,
  workflowInfo
} from "@temporalio/workflow"
import type { DeploymentActivities } from "../activities/types.ts"
import { type DeploymentInput, type DeploymentResult, type DeploymentState, SEARCH_ATTRIBUTES } from "../deployment-input.ts"

/**
 * Canary deployment workflow — deterministic, plain TypeScript, ZERO Effect.
 *
 * Rollback is a saga (N2): the first traffic shift registers a compensation
 * that restores the previous version. Every non-success termination — a
 * threshold breach, an abort, or an unexpected failure — runs the compensation
 * stack, so traffic is never left stranded on a bad version. approve/abort are
 * validated Updates; progress is exposed through the `status` query.
 *
 * Activity shapes (N4, D16): the health check is a **local activity** (a quick
 * call, no task-queue round-trip); monitoring is a regular activity that
 * **heartbeats** and runs inside a **CancellationScope**, so an abort cancels
 * the in-flight monitor immediately instead of waiting for it to finish.
 */

// Traffic shifts and notifications: ordinary activities.
const acts = proxyActivities<Pick<DeploymentActivities, "setTrafficWeight" | "notify" | "recordOutcome">>({
  startToCloseTimeout: "5 minutes",
  retry: { maximumAttempts: 3 }
})

// Monitoring is long-running and heartbeats, so it can be cancelled promptly and
// resumed if a worker dies mid-window.
const monitorActs = proxyActivities<Pick<DeploymentActivities, "monitorStep">>({
  startToCloseTimeout: "1 hour",
  heartbeatTimeout: "30 seconds",
  retry: { maximumAttempts: 3 }
})

// The health probe is fast and side-effect-free: a local activity avoids a
// separate activity task and its scheduling latency.
const localActs = proxyLocalActivities<Pick<DeploymentActivities, "healthCheck">>({
  startToCloseTimeout: "10 seconds",
  retry: { maximumAttempts: 3 }
})

/** Approve advancing past a manual-approval gate (rejected if none is open). */
export const approveUpdate = defineUpdate<void, []>("approve")
/** Abort an in-flight deployment (rejected once it has finished). */
export const abortUpdate = defineUpdate<void, []>("abort")
/**
 * Abort via a signal — the fire-and-forget form used by a parent workflow to
 * abort a child (child handles can signal but not update). Same effect as the
 * `abort` update, without the synchronous confirmation.
 */
export const abortSignal = defineSignal("abortSignal")
/** Read the live deployment state. */
export const statusQuery = defineQuery<DeploymentState>("status")

const asApplicationFailure = (error: unknown): ApplicationFailure | undefined => {
  if (error instanceof ApplicationFailure) {
    return error
  }
  if (error instanceof ActivityFailure && error.cause instanceof ApplicationFailure) {
    return error.cause
  }
  return undefined
}

export async function deploymentWorkflow(input: DeploymentInput): Promise<DeploymentResult> {
  // Set when this run resumed from a continue-as-new mid-rollout (N4/D16).
  const resume = input.resumeFrom
  const completedBefore = resume?.completedSteps ?? 0

  let state: DeploymentState = {
    phase: resume === undefined ? "health-checking" : "shifting",
    service: input.service,
    version: input.version,
    currentPercent: resume?.lastPercent ?? 0,
    stepIndex: completedBefore,
    totalSteps: completedBefore + input.steps.length
  }
  let aborted = false
  let approved = false
  // Set while a monitor activity is in flight, so abort can cancel it at once.
  let cancelMonitor: (() => void) | undefined

  // Saga: undo actions to run (LIFO) on any non-success termination.
  const compensations: Array<() => Promise<void>> = []
  const compensate = async (): Promise<void> => {
    state = { ...state, phase: "rolling-back" }
    while (compensations.length > 0) {
      const undo = compensations.pop()!
      try {
        await undo()
      } catch (error) {
        log.error("compensation failed", { error: String(error) })
      }
    }
  }

  upsertSearchAttributes({
    [SEARCH_ATTRIBUTES.service]: [input.service],
    [SEARCH_ATTRIBUTES.version]: [input.version],
    [SEARCH_ATTRIBUTES.status]: ["running"]
  })

  const abort = (): void => {
    aborted = true
    // If we are mid-window, cancel the monitor so the abort takes effect now.
    cancelMonitor?.()
  }

  setHandler(statusQuery, () => state)
  setHandler(abortSignal, abort)
  setHandler(abortUpdate, abort, {
    validator: () => {
      if (state.phase === "done") {
        throw new Error("deployment already finished")
      }
    }
  })
  setHandler(approveUpdate, () => {
    approved = true
  }, {
    validator: () => {
      if (state.phase !== "awaiting-approval") {
        throw new Error("no approval gate is currently open")
      }
    }
  })

  let result: DeploymentResult
  try {
    result = await runCanary()
  } catch (error) {
    const failure = asApplicationFailure(error)
    if (failure === undefined) {
      throw error
    }
    log.warn("deployment failed", { type: failure.type })
    await compensate() // restore traffic if anything was shifted before the failure
    result = {
      kind: "Failed",
      service: input.service,
      reason: `${failure.type ?? "error"}: ${failure.message}`
    }
  }

  state = { ...state, phase: "done", outcome: result.kind }
  upsertSearchAttributes({ [SEARCH_ATTRIBUTES.status]: [result.kind] })
  await acts.recordOutcome(result.kind)
  return result

  async function runCanary(): Promise<DeploymentResult> {
    // 1. Health-check the new version before shifting any traffic (local activity).
    //    A resumed run already passed this in its first incarnation.
    if (resume === undefined) {
      await localActs.healthCheck({ service: input.service, version: input.version })
    } else if (resume.trafficShifted) {
      // Traffic was already diverted before the continue-as-new — re-arm the
      // rollback compensation so a breach in this run still restores traffic.
      compensations.push(() =>
        acts.setTrafficWeight({ service: input.service, version: input.previousVersion, weight: 100 }))
    }

    // 2. Progressive canary steps.
    let lastPercent = resume?.lastPercent ?? 0
    for (let stepIndex = 0; stepIndex < input.steps.length; stepIndex++) {
      const step = input.steps[stepIndex]!
      if (aborted) {
        await compensate()
        return { kind: "Aborted", service: input.service, atPercent: lastPercent }
      }
      lastPercent = step.percent

      state = { ...state, phase: "shifting", currentPercent: step.percent, stepIndex: completedBefore + stepIndex }
      await acts.setTrafficWeight({
        service: input.service,
        version: input.version,
        weight: step.percent
      })
      // Register the compensation the first time traffic diverts to the new version.
      if (compensations.length === 0) {
        compensations.push(() =>
          acts.setTrafficWeight({
            service: input.service,
            version: input.previousVersion,
            weight: 100
          }))
      }

      state = { ...state, phase: "monitoring" }
      // Run the monitor in its own scope so an abort can cancel it mid-window.
      const monitorScope = new CancellationScope()
      cancelMonitor = () => monitorScope.cancel()
      let evaluation: Awaited<ReturnType<DeploymentActivities["monitorStep"]>>
      try {
        evaluation = await monitorScope.run(() =>
          monitorActs.monitorStep({
            service: input.service,
            version: input.version,
            windowMs: step.monitorMs,
            pollIntervalMs: input.pollIntervalMs,
            rules: input.rules
          }))
      } catch (error) {
        if (aborted && isCancellation(error)) {
          await compensate()
          return { kind: "Aborted", service: input.service, atPercent: step.percent }
        }
        throw error
      } finally {
        cancelMonitor = undefined
      }

      if (evaluation._tag === "Breached") {
        log.warn("threshold breached, rolling back", { atPercent: step.percent })
        await compensate()
        await acts.notify({
          kind: "rolled-back",
          service: input.service,
          message: `regression at ${step.percent}% — rolled back to ${input.previousVersion}`
        })
        return {
          kind: "RolledBack",
          service: input.service,
          toVersion: input.previousVersion,
          atPercent: step.percent,
          breaches: evaluation.breaches
        }
      }

      // 3. Optional manual-approval gate.
      if (step.requiresApproval) {
        state = { ...state, phase: "awaiting-approval" }
        await condition(() => approved || aborted, step.approvalTimeoutMs)
        if (aborted) {
          await compensate()
          return { kind: "Aborted", service: input.service, atPercent: step.percent }
        }
        approved = false
      }

      // 4. Bound history: continue-as-new with the remaining steps when Temporal
      //    suggests it (long history) or an explicit step bound is reached.
      const isLastStep = stepIndex === input.steps.length - 1
      const reachedBound = input.continueAsNewAfterSteps !== undefined &&
        stepIndex + 1 >= input.continueAsNewAfterSteps
      if (!isLastStep && (workflowInfo().continueAsNewSuggested || reachedBound)) {
        await continueAsNew<typeof deploymentWorkflow>({
          ...input,
          steps: input.steps.slice(stepIndex + 1),
          resumeFrom: {
            completedSteps: completedBefore + stepIndex + 1,
            trafficShifted: compensations.length > 0,
            lastPercent: step.percent
          }
        })
      }
    }

    // 5. Full rollout succeeded — commit (drop compensations, keep new version live).
    compensations.length = 0
    await acts.notify({ kind: "succeeded", service: input.service, message: input.version })
    return { kind: "Succeeded", service: input.service, version: input.version }
  }
}

// Pin an in-flight deployment to the worker version that started it (N4/D15), so
// a rolling worker upgrade never changes the code running a canary mid-flight.
setWorkflowOptions({ versioningBehavior: "PINNED" }, deploymentWorkflow)
