import {
  ActivityFailure,
  ApplicationFailure,
  condition,
  defineSignal,
  log,
  proxyActivities,
  setHandler
} from "@temporalio/workflow"
import type { DeploymentActivities } from "../activities/types.ts"
import type { DeploymentInput, DeploymentResult } from "../deployment-input.ts"

/**
 * Canary deployment workflow — deterministic, plain TypeScript, ZERO Effect.
 * It only sequences activities and branches on the plain tagged results they
 * return; all I/O and the threshold rule live in activities.
 *
 * N0 uses signals for approve/abort; N2 upgrades these to validated Updates.
 */

const acts = proxyActivities<DeploymentActivities>({
  startToCloseTimeout: "5 minutes",
  retry: { maximumAttempts: 3 }
})

/** Manually abort the deployment. */
export const abortSignal = defineSignal("abort")
/** Approve advancing past a manual-approval gate. */
export const approveSignal = defineSignal("approve")

/**
 * Extract the typed `ApplicationFailure` an activity produced (its `type` is the
 * Effect error's `_tag`). Returns undefined for anything else — an unexpected
 * defect must not be laundered into a business outcome.
 */
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
  let aborted = false
  let approved = false
  setHandler(abortSignal, () => {
    aborted = true
  })
  setHandler(approveSignal, () => {
    approved = true
  })

  let result: DeploymentResult
  try {
    result = await runCanary(input, () => aborted, () => approved, () => {
      approved = false
    })
  } catch (error) {
    // Typed activity failures become a clean Failed outcome; real defects rethrow.
    const failure = asApplicationFailure(error)
    if (failure === undefined) {
      throw error
    }
    log.warn("deployment failed", { type: failure.type })
    result = {
      kind: "Failed",
      service: input.service,
      reason: `${failure.type ?? "error"}: ${failure.message}`
    }
  }

  await acts.recordOutcome(result.kind)
  return result
}

async function runCanary(
  input: DeploymentInput,
  isAborted: () => boolean,
  isApproved: () => boolean,
  clearApproval: () => void
): Promise<DeploymentResult> {
  // 1. Health-check the new version before shifting any traffic.
  await acts.healthCheck({ service: input.service, version: input.version })

  // 2. Progressive canary steps.
  let lastPercent = 0
  for (const step of input.steps) {
    if (isAborted()) {
      return { kind: "Aborted", service: input.service, atPercent: lastPercent }
    }
    lastPercent = step.percent

    await acts.setTrafficWeight({
      service: input.service,
      version: input.version,
      weight: step.percent
    })

    // The monitorStep activity polls metrics over the window and returns early
    // on a breach — the waiting happens inside the activity, not here.
    const evaluation = await acts.monitorStep({
      service: input.service,
      version: input.version,
      windowMs: step.monitorMs,
      pollIntervalMs: input.pollIntervalMs,
      rules: input.rules
    })

    if (evaluation._tag === "Breached") {
      log.warn("threshold breached, rolling back", { atPercent: step.percent })
      await acts.setTrafficWeight({
        service: input.service,
        version: input.previousVersion,
        weight: 100
      })
      await acts.notify({
        kind: "rolled-back",
        service: input.service,
        message: `regression at ${step.percent}% — rolled back to ${input.previousVersion}`
      })
      return {
        kind: "RolledBack",
        service: input.service,
        toVersion: input.previousVersion,
        atPercent: step.percent
      }
    }

    // 3. Optional manual-approval gate.
    if (step.requiresApproval) {
      await condition(() => isApproved() || isAborted(), step.approvalTimeoutMs)
      if (isAborted()) {
        return { kind: "Aborted", service: input.service, atPercent: step.percent }
      }
      clearApproval()
    }
  }

  // 4. Full rollout succeeded.
  await acts.notify({ kind: "succeeded", service: input.service, message: input.version })
  return { kind: "Succeeded", service: input.service, version: input.version }
}
