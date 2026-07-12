import {
  ActivityFailure,
  ApplicationFailure,
  condition,
  defineQuery,
  defineUpdate,
  log,
  proxyActivities,
  setHandler,
  upsertSearchAttributes
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
 */

const acts = proxyActivities<DeploymentActivities>({
  startToCloseTimeout: "5 minutes",
  retry: { maximumAttempts: 3 }
})

/** Approve advancing past a manual-approval gate (rejected if none is open). */
export const approveUpdate = defineUpdate<void, []>("approve")
/** Abort an in-flight deployment (rejected once it has finished). */
export const abortUpdate = defineUpdate<void, []>("abort")
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
  let state: DeploymentState = {
    phase: "health-checking",
    service: input.service,
    version: input.version,
    currentPercent: 0,
    stepIndex: 0,
    totalSteps: input.steps.length
  }
  let aborted = false
  let approved = false

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

  setHandler(statusQuery, () => state)
  setHandler(abortUpdate, () => {
    aborted = true
  }, {
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
    // 1. Health-check the new version before shifting any traffic.
    await acts.healthCheck({ service: input.service, version: input.version })

    // 2. Progressive canary steps.
    let lastPercent = 0
    for (let stepIndex = 0; stepIndex < input.steps.length; stepIndex++) {
      const step = input.steps[stepIndex]!
      if (aborted) {
        await compensate()
        return { kind: "Aborted", service: input.service, atPercent: lastPercent }
      }
      lastPercent = step.percent

      state = { ...state, phase: "shifting", currentPercent: step.percent, stepIndex }
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
      const evaluation = await acts.monitorStep({
        service: input.service,
        version: input.version,
        windowMs: step.monitorMs,
        pollIntervalMs: input.pollIntervalMs,
        rules: input.rules
      })

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
    }

    // 4. Full rollout succeeded — commit (drop compensations, keep new version live).
    compensations.length = 0
    await acts.notify({ kind: "succeeded", service: input.service, message: input.version })
    return { kind: "Succeeded", service: input.service, version: input.version }
  }
}
