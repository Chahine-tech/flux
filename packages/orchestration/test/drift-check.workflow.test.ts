import { fileURLToPath } from "node:url"
import { TestWorkflowEnvironment } from "@temporalio/testing"
import { Worker } from "@temporalio/worker"
import { afterAll, beforeAll, describe, expect, it } from "vitest"
import type { DeploymentActivities } from "../src/activities/types.ts"
import type { DriftCheckInput, DriftReport } from "../src/deployment-input.ts"

/**
 * Drift-check workflow against a time-skipping Temporal server (N4/D17): it reads
 * the actual routing (mocked) and compares it to the desired weights, optionally
 * reconciling. Activities are mocked to control the "actual" state.
 */

const TASK_QUEUE = "flux-drift-test"
const workflowsPath = fileURLToPath(new URL("../src/workflows/index.ts", import.meta.url))

const baseActivities = (): DeploymentActivities => ({
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
}, 60_000)

afterAll(async () => {
  await env?.teardown()
})

const run = async (input: DriftCheckInput, activities: DeploymentActivities): Promise<DriftReport> => {
  const worker = await Worker.create({
    connection: env.nativeConnection,
    namespace: env.namespace ?? "default",
    taskQueue: TASK_QUEUE,
    workflowsPath,
    activities
  })
  return worker.runUntil(
    env.client.workflow.execute("driftCheck", {
      taskQueue: TASK_QUEUE,
      workflowId: `drift-${Date.now()}-${Math.random().toString(36).slice(2)}`,
      args: [input]
    })
  ) as Promise<DriftReport>
}

describe("driftCheck", () => {
  it("reports no drift when the actual routing matches the desired", async () => {
    const result = await run(
      { service: "api", desired: [{ version: "v2", weight: 100 }], reconcile: true },
      { ...baseActivities(), readRouterState: async () => [{ version: "v2", weight: 100 }] }
    )
    expect(result.drifted).toBe(false)
    expect(result.reconciled).toBe(false)
  })

  it("reconciles by re-applying the desired weights when the router has drifted", async () => {
    const shifts: Array<{ service: string; version: string; weight: number }> = []
    const result = await run(
      { service: "api", desired: [{ version: "v2", weight: 100 }], reconcile: true },
      {
        ...baseActivities(),
        // Router actually serving the old version — drift.
        readRouterState: async () => [{ version: "v1", weight: 100 }],
        setTrafficWeight: async (p) => {
          shifts.push(p)
        }
      }
    )
    expect(result.drifted).toBe(true)
    expect(result.reconciled).toBe(true)
    expect(shifts).toEqual([{ service: "api", version: "v2", weight: 100 }])
  })

  it("detects drift without reconciling when reconcile is off", async () => {
    const shifts: Array<unknown> = []
    const result = await run(
      { service: "api", desired: [{ version: "v2", weight: 100 }], reconcile: false },
      {
        ...baseActivities(),
        readRouterState: async () => [{ version: "v1", weight: 100 }],
        setTrafficWeight: async (p) => {
          shifts.push(p)
        }
      }
    )
    expect(result.drifted).toBe(true)
    expect(result.reconciled).toBe(false)
    expect(shifts).toHaveLength(0)
  })
})
