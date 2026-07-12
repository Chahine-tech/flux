import { condition, defineSignal, log, proxyActivities, setHandler, sleep } from "@temporalio/workflow"
import type { DeploymentActivities } from "../activities/types.ts"
import type { DeploymentInput, DeploymentResult } from "../deployment-input.ts"

/**
 * Canary deployment workflow — deterministic, plain TypeScript, ZERO Effect
 * (ARCHITECTURE.md D6). It only sequences activities and branches on the plain
 * tagged results they return; all I/O and the threshold rule live in activities.
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

export async function deploymentWorkflow(input: DeploymentInput): Promise<DeploymentResult> {
  let aborted = false
  let approved = false
  setHandler(abortSignal, () => {
    aborted = true
  })
  setHandler(approveSignal, () => {
    approved = true
  })

  // 1. Health-check the new version before shifting any traffic.
  await acts.healthCheck({ service: input.service, version: input.version })

  // 2. Progressive canary steps.
  let lastPercent = 0
  for (const step of input.steps) {
    if (aborted) {
      return { kind: "Aborted", service: input.service, atPercent: lastPercent }
    }
    lastPercent = step.percent

    await acts.setTrafficWeight({
      service: input.service,
      version: input.version,
      weight: step.percent
    })

    if (step.monitorMs > 0) {
      await sleep(step.monitorMs)
    }

    const evaluation = await acts.monitorStep({
      service: input.service,
      version: input.version,
      windowMs: step.monitorMs,
      thresholds: input.thresholds
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
      await condition(() => approved || aborted, step.approvalTimeoutMs)
      if (aborted) {
        return { kind: "Aborted", service: input.service, atPercent: step.percent }
      }
      approved = false
    }
  }

  // 4. Full rollout succeeded.
  await acts.notify({ kind: "succeeded", service: input.service, message: input.version })
  return { kind: "Succeeded", service: input.service, version: input.version }
}
