import { fileURLToPath } from "node:url"
import { Context } from "@temporalio/activity"
import { ApplicationFailure } from "@temporalio/common"
import { TestWorkflowEnvironment } from "@temporalio/testing"
import { Worker } from "@temporalio/worker"
import { afterAll, beforeAll, describe, expect, it } from "vitest"
import { type DeploymentInput, type DeploymentResult, type DeploymentState, SEARCH_ATTRIBUTES } from "../src/deployment-input.ts"
import type { DeploymentActivities } from "../src/activities/types.ts"

// temporal.api.enums.v1.IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD
const KEYWORD = 2

/**
 * Versioned workflow tests against a time-skipping Temporal server. Activities
 * are mocked so we test the workflow's own logic — canary sequencing, the
 * rollback decision, and the typed-failure -> Failed outcome — deterministically.
 */

const TASK_QUEUE = "flux-workflow-test"
const workflowsPath = fileURLToPath(new URL("../src/workflows/index.ts", import.meta.url))

const baseInput: DeploymentInput = {
  service: "api",
  version: "v2.1.0",
  previousVersion: "v2.0.8",
  steps: [
    { percent: 10, monitorMs: 0, requiresApproval: false },
    { percent: 50, monitorMs: 0, requiresApproval: false },
    { percent: 100, monitorMs: 0, requiresApproval: false }
  ],
  rules: [{ name: "errorRate", query: "q", max: 0.01 }],
  pollIntervalMs: 100
}

// Base mock: everything succeeds, metrics always within budget.
const okActivities = (): DeploymentActivities => ({
  healthCheck: async () => {},
  setTrafficWeight: async () => {},
  monitorStep: async () => ({ _tag: "Within" }),
  notify: async () => {},
  readRouterState: async () => [],
  recordOutcome: async () => {}
})

let env: TestWorkflowEnvironment

beforeAll(async () => {
  env = await TestWorkflowEnvironment.createTimeSkipping()
  // The workflow upserts these, so they must exist on the ephemeral server too.
  await env.connection.operatorService.addSearchAttributes({
    namespace: env.namespace ?? "default",
    searchAttributes: {
      [SEARCH_ATTRIBUTES.service]: KEYWORD,
      [SEARCH_ATTRIBUTES.version]: KEYWORD,
      [SEARCH_ATTRIBUTES.status]: KEYWORD
    }
  }).catch(() => {})
}, 60_000)

afterAll(async () => {
  await env?.teardown()
})

const run = async (
  activities: DeploymentActivities,
  input: DeploymentInput = baseInput
): Promise<DeploymentResult> => {
  const worker = await Worker.create({
    connection: env.nativeConnection,
    namespace: env.namespace ?? "default",
    taskQueue: TASK_QUEUE,
    workflowsPath,
    activities
  })
  return worker.runUntil(
    env.client.workflow.execute("deploymentWorkflow", {
      taskQueue: TASK_QUEUE,
      workflowId: `wf-${Date.now()}-${Math.random().toString(36).slice(2)}`,
      args: [input]
    })
  ) as Promise<DeploymentResult>
}

describe("deploymentWorkflow", () => {
  it("succeeds when every step stays within budget", async () => {
    const result = await run(okActivities())
    expect(result.kind).toBe("Succeeded")
  })

  it("rolls back when a step breaches the threshold", async () => {
    let step = 0
    const result = await run({
      ...okActivities(),
      monitorStep: async () => (++step >= 2 ? { _tag: "Breached", breaches: [{ metric: "errorRate", observed: 0.05, limit: 0.01 }] } : { _tag: "Within" })
    })
    expect(result.kind).toBe("RolledBack")
    if (result.kind === "RolledBack") {
      expect(result.atPercent).toBe(50)
      expect(result.toVersion).toBe("v2.0.8")
    }
  })

  it("parks at an approval gate, reflects it in the query, and advances on approve", async () => {
    const input: DeploymentInput = {
      ...baseInput,
      steps: [
        { percent: 50, monitorMs: 0, requiresApproval: true },
        { percent: 100, monitorMs: 0, requiresApproval: false }
      ]
    }
    const worker = await Worker.create({
      connection: env.nativeConnection,
      namespace: env.namespace ?? "default",
      taskQueue: TASK_QUEUE,
      workflowsPath,
      activities: okActivities()
    })
    const result = (await worker.runUntil(async () => {
      const handle = await env.client.workflow.start("deploymentWorkflow", {
        taskQueue: TASK_QUEUE,
        workflowId: `wf-approve-${Date.now()}`,
        args: [input]
      })
      for (let i = 0; i < 50; i++) {
        const s = await handle.query<DeploymentState>("status")
        if (s.phase === "awaiting-approval") break
        await new Promise((r) => setTimeout(r, 50))
      }
      const parked = await handle.query<DeploymentState>("status")
      expect(parked.phase).toBe("awaiting-approval")
      expect(parked.currentPercent).toBe(50)
      await handle.executeUpdate("approve")
      return handle.result()
    })) as DeploymentResult
    expect(result.kind).toBe("Succeeded")
  })

  it("restores traffic to the previous version when aborted at a gate (saga)", async () => {
    const shifts: Array<{ readonly version: string; readonly weight: number }> = []
    const input: DeploymentInput = {
      ...baseInput,
      steps: [{ percent: 50, monitorMs: 0, requiresApproval: true }]
    }
    const worker = await Worker.create({
      connection: env.nativeConnection,
      namespace: env.namespace ?? "default",
      taskQueue: TASK_QUEUE,
      workflowsPath,
      activities: {
        ...okActivities(),
        setTrafficWeight: async (p: { version: string; weight: number }) => {
          shifts.push({ version: p.version, weight: p.weight })
        }
      } satisfies DeploymentActivities
    })
    const result = (await worker.runUntil(async () => {
      const handle = await env.client.workflow.start("deploymentWorkflow", {
        taskQueue: TASK_QUEUE,
        workflowId: `wf-abort-${Date.now()}`,
        args: [input]
      })
      for (let i = 0; i < 50; i++) {
        if ((await handle.query<DeploymentState>("status")).phase === "awaiting-approval") break
        await new Promise((r) => setTimeout(r, 50))
      }
      await handle.executeUpdate("abort")
      return handle.result()
    })) as DeploymentResult
    expect(result.kind).toBe("Aborted")
    // The compensation restored the previous version to 100% traffic.
    expect(shifts.at(-1)).toEqual({ version: "v2.0.8", weight: 100 })
  })

  it("cancels an in-flight monitor on abort, instead of waiting out the window (N4)", async () => {
    // A monitor that never returns on its own: it heartbeats and waits to be
    // cancelled. Without the workflow's CancellationScope, the abort would set a
    // flag but the workflow would block here forever.
    const cancellableMonitor = async (): Promise<never> => {
      for (;;) {
        Context.current().heartbeat()
        await Context.current().sleep(50) // throws CancelledFailure once cancelled
      }
    }
    const input: DeploymentInput = {
      ...baseInput,
      steps: [{ percent: 50, monitorMs: 600_000, requiresApproval: false }]
    }
    const worker = await Worker.create({
      connection: env.nativeConnection,
      namespace: env.namespace ?? "default",
      taskQueue: TASK_QUEUE,
      workflowsPath,
      activities: { ...okActivities(), monitorStep: cancellableMonitor }
    })
    const result = (await worker.runUntil(async () => {
      const handle = await env.client.workflow.start("deploymentWorkflow", {
        taskQueue: TASK_QUEUE,
        workflowId: `wf-cancel-${Date.now()}`,
        args: [input]
      })
      for (let i = 0; i < 100; i++) {
        if ((await handle.query<DeploymentState>("status")).phase === "monitoring") break
        await new Promise((r) => setTimeout(r, 50))
      }
      await handle.executeUpdate("abort")
      return handle.result()
    })) as DeploymentResult
    expect(result.kind).toBe("Aborted")
    if (result.kind === "Aborted") {
      expect(result.atPercent).toBe(50)
    }
  })

  it("continues-as-new mid-rollout and still completes every step (N4)", async () => {
    const shifts: Array<number> = []
    const input: DeploymentInput = {
      ...baseInput,
      // Four steps, but bound each run to two → one continue-as-new in the middle.
      steps: [
        { percent: 10, monitorMs: 0, requiresApproval: false },
        { percent: 40, monitorMs: 0, requiresApproval: false },
        { percent: 70, monitorMs: 0, requiresApproval: false },
        { percent: 100, monitorMs: 0, requiresApproval: false }
      ],
      continueAsNewAfterSteps: 2
    }
    const worker = await Worker.create({
      connection: env.nativeConnection,
      namespace: env.namespace ?? "default",
      taskQueue: TASK_QUEUE,
      workflowsPath,
      activities: {
        ...okActivities(),
        setTrafficWeight: async (p: { weight: number }) => {
          shifts.push(p.weight)
        }
      } satisfies DeploymentActivities
    })
    const result = (await worker.runUntil(
      env.client.workflow.execute("deploymentWorkflow", {
        taskQueue: TASK_QUEUE,
        workflowId: `wf-can-${Date.now()}`,
        args: [input]
      })
    )) as DeploymentResult

    expect(result.kind).toBe("Succeeded")
    // Every step ran across the continue-as-new boundary.
    expect(shifts).toEqual([10, 40, 70, 100])
  })

  it("turns a non-retryable activity failure into a Failed outcome", async () => {
    const result = await run({
      ...okActivities(),
      healthCheck: async () => {
        throw ApplicationFailure.nonRetryable("probe 503", "HealthCheckFailed")
      }
    })
    expect(result.kind).toBe("Failed")
    if (result.kind === "Failed") {
      expect(result.reason).toContain("HealthCheckFailed")
    }
  })
}, 120_000)
