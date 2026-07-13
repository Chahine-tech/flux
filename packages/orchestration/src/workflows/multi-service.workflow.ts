import { type ChildWorkflowHandle, defineQuery, log, setHandler, startChild, workflowInfo } from "@temporalio/workflow"
import type { DeploymentResult, MultiServiceInput, MultiServiceResult, MultiServiceState } from "../deployment-input.ts"
import { abortSignal, deploymentWorkflow } from "./deployment.workflow.ts"

/**
 * Multi-service rollout — deterministic parent over N `deploymentWorkflow`
 * children, one per service (N4/D13).
 *
 * Concurrency ("K services at a time") is a plain-TS worker pool over a queue —
 * no Effect in the deterministic parent (D6). Fail-fast: the first child that
 * does not succeed aborts every in-flight sibling via the child's `abortSignal`,
 * so each stops and rolls back its own traffic through its saga. Children that
 * had already finished are left as-is (undoing a completed rollout is a separate,
 * risky operation) — the aggregate simply reports `SomeFailed`.
 */

/** Read the aggregate rollout state. */
export const multiStatusQuery = defineQuery<MultiServiceState>("status")

type Child = ChildWorkflowHandle<typeof deploymentWorkflow>

export async function multiServiceDeployment(input: MultiServiceInput): Promise<MultiServiceResult> {
  const parentId = workflowInfo().workflowId
  const perService: Array<{ readonly service: string; readonly result: DeploymentResult }> = []
  const inflight = new Map<string, Child>()
  let failed = false

  let state: MultiServiceState = { total: input.services.length, running: 0, succeeded: 0, failed: 0 }
  setHandler(multiStatusQuery, () => state)

  const abortSiblings = async (except: string): Promise<void> => {
    for (const [service, handle] of inflight) {
      if (service === except) continue
      await handle.signal(abortSignal).catch((error) => log.warn("sibling abort failed", { service, error: String(error) }))
    }
  }

  const runOne = async (serviceInput: MultiServiceInput["services"][number]): Promise<void> => {
    const handle = await startChild(deploymentWorkflow, {
      workflowId: `${parentId}-${serviceInput.service}`,
      args: [serviceInput]
    })
    inflight.set(serviceInput.service, handle)
    state = { ...state, running: state.running + 1 }
    try {
      const result = await handle.result()
      perService.push({ service: serviceInput.service, result })
      const ok = result.kind === "Succeeded"
      state = {
        ...state,
        running: state.running - 1,
        succeeded: state.succeeded + (ok ? 1 : 0),
        failed: state.failed + (ok ? 0 : 1)
      }
      if (!ok && input.failFast && !failed) {
        failed = true
        await abortSiblings(serviceInput.service)
      }
    } finally {
      inflight.delete(serviceInput.service)
    }
  }

  // Deterministic worker pool: `concurrency` workers pull from a shared queue.
  const queue = [...input.services]
  const poolWorker = async (): Promise<void> => {
    while (queue.length > 0 && !(input.failFast && failed)) {
      await runOne(queue.shift()!)
    }
  }
  const concurrency = Math.max(1, Math.min(input.maxConcurrency, input.services.length))
  await Promise.all(Array.from({ length: concurrency }, () => poolWorker()))

  const allSucceeded = perService.length > 0 && perService.every((entry) => entry.result.kind === "Succeeded")
  return { kind: allSucceeded ? "AllSucceeded" : "SomeFailed", perService }
}
