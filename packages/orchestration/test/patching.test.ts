import { fileURLToPath } from "node:url"
import { TestWorkflowEnvironment } from "@temporalio/testing"
import { bundleWorkflowCode, Worker } from "@temporalio/worker"
import { afterAll, beforeAll, describe, expect, it } from "vitest"
import { type DeploymentInput, type DeploymentResult, SEARCH_ATTRIBUTES } from "../src/deployment-input.ts"
import type { DeploymentActivities } from "../src/activities/types.ts"

// temporal.api.enums.v1.IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD
const KEYWORD = 2

/**
 * D26, the forward half: a NEW execution takes the patched path — the
 * `started` notification fires first, and the recorded history carries the
 * patch marker the guard wrote. The backward half (histories recorded before
 * the patch replay through the else-branch) is proven by the D22 lock in
 * `replay.test.ts`, whose fixtures predate the patch; the negative proof
 * (same edit without the guard → `DeterminismViolationError`) was run and is
 * documented in ARCHITECTURE.md D26.
 */

const TASK_QUEUE = "flux-patching"
const workflowsPath = fileURLToPath(new URL("../src/workflows/index.ts", import.meta.url))

const input: DeploymentInput = {
  service: "api",
  version: "v2.1.0",
  previousVersion: "v2.0.8",
  steps: [{ percent: 100, monitorMs: 0, requiresApproval: false }],
  rules: [{ name: "errorRate", query: "q", max: 0.01 }],
  pollIntervalMs: 100
}

let env: TestWorkflowEnvironment
let workflowBundle: Awaited<ReturnType<typeof bundleWorkflowCode>>

beforeAll(async () => {
  env = await TestWorkflowEnvironment.createTimeSkipping()
  workflowBundle = await bundleWorkflowCode({ workflowsPath })
  await env.connection.operatorService.addSearchAttributes({
    namespace: env.namespace ?? "default",
    searchAttributes: {
      [SEARCH_ATTRIBUTES.service]: KEYWORD,
      [SEARCH_ATTRIBUTES.version]: KEYWORD,
      [SEARCH_ATTRIBUTES.status]: KEYWORD
    }
  }).catch((error: unknown) => {
    if (!/already exist/i.test(String(error))) throw error
  })
}, 60_000)

afterAll(async () => {
  await env?.teardown()
})

describe("workflow patching (D26)", () => {
  it("a new execution takes the patched path: started notification first, patch marker in history", async () => {
    const notifications: Array<{ kind: string; service: string }> = []
    const activities: DeploymentActivities = {
      healthCheck: async () => {},
      setTrafficWeight: async () => {},
      monitorStep: async () => ({ _tag: "Within" }),
      notify: async (n: { kind: string; service: string }) => {
        notifications.push({ kind: n.kind, service: n.service })
      },
      readRouterState: async () => [],
      recordOutcome: async () => {}
    }
    const worker = await Worker.create({
      connection: env.nativeConnection,
      namespace: env.namespace ?? "default",
      taskQueue: TASK_QUEUE,
      workflowBundle,
      activities
    })

    const workflowId = `patched-${Date.now()}`
    const result = (await worker.runUntil(
      env.client.workflow.execute("deploymentWorkflow", {
        taskQueue: TASK_QUEUE,
        workflowId,
        args: [input]
      })
    )) as DeploymentResult
    expect(result.kind).toBe("Succeeded")

    // The genuinely new behavior: the deployment announced itself.
    expect(notifications[0]).toEqual({ kind: "started", service: "api" })

    // The mechanism, on the wire: the guard recorded a patch marker event in
    // the history — what lets old code paths coexist with this one.
    const history = await env.client.workflow.getHandle(workflowId).fetchHistory()
    const marker = (history.events ?? []).find(
      (e) => e.markerRecordedEventAttributes?.markerName === "core_patch"
    )
    expect(marker).toBeDefined()
  }, 60_000)
})
