import { fileURLToPath } from "node:url"
import { Context } from "@temporalio/activity"
import { ApplicationFailure } from "@temporalio/common"
import { TestWorkflowEnvironment } from "@temporalio/testing"
import { bundleWorkflowCode, Worker } from "@temporalio/worker"
import { afterAll, beforeAll, describe, expect, it } from "vitest"
import type { DeploymentActivities } from "../src/activities/types.ts"
import { type DeploymentInput, type MultiServiceInput, type MultiServiceResult, SEARCH_ATTRIBUTES } from "../src/deployment-input.ts"

/**
 * Multi-service parent workflow against a time-skipping Temporal server (N4/D13):
 * it starts one real `deploymentWorkflow` child per service and coordinates them.
 * Activities are mocked so we test the parent's fan-out and fail-fast policy.
 */

const KEYWORD = 2
const TASK_QUEUE = "flux-multi-test"
const workflowsPath = fileURLToPath(new URL("../src/workflows/index.ts", import.meta.url))

const service = (name: string, monitorMs: number, requiresApproval = false): DeploymentInput => ({
  service: name,
  version: "v2",
  previousVersion: "v1",
  steps: [{ percent: 100, monitorMs, requiresApproval }],
  rules: [{ name: "errorRate", query: "q", max: 0.01 }],
  pollIntervalMs: 50
})

const okActivities = (): DeploymentActivities => ({
  healthCheck: async () => {},
  setTrafficWeight: async () => {},
  monitorStep: async () => ({ _tag: "Within" }),
  notify: async () => {},
  readRouterState: async () => [],
  recordOutcome: async () => {}
})

let env: TestWorkflowEnvironment
let workflowBundle: Awaited<ReturnType<typeof bundleWorkflowCode>>

beforeAll(async () => {
  env = await TestWorkflowEnvironment.createTimeSkipping()
  // Bundle once for the whole file, reused by every worker (see
  // deployment.workflow.test.ts for why).
  workflowBundle = await bundleWorkflowCode({ workflowsPath })
  await env.connection.operatorService.addSearchAttributes({
    namespace: env.namespace ?? "default",
    searchAttributes: {
      [SEARCH_ATTRIBUTES.service]: KEYWORD,
      [SEARCH_ATTRIBUTES.version]: KEYWORD,
      [SEARCH_ATTRIBUTES.status]: KEYWORD
    }
  }).catch((error: unknown) => {
    // Ignore "already registered"; surface anything unexpected.
    if (!/already exist/i.test(String(error))) throw error
  })
}, 60_000)

afterAll(async () => {
  await env?.teardown()
})

const run = async (input: MultiServiceInput, activities: DeploymentActivities): Promise<MultiServiceResult> => {
  const worker = await Worker.create({
    connection: env.nativeConnection,
    namespace: env.namespace ?? "default",
    taskQueue: TASK_QUEUE,
    workflowBundle,
    activities
  })
  return worker.runUntil(
    env.client.workflow.execute("multiServiceDeployment", {
      taskQueue: TASK_QUEUE,
      workflowId: `multi-${Date.now()}-${Math.random().toString(36).slice(2)}`,
      args: [input]
    })
  ) as Promise<MultiServiceResult>
}

describe("multiServiceDeployment", () => {
  it("rolls out every service and reports AllSucceeded", async () => {
    const input: MultiServiceInput = {
      services: [service("api", 0), service("web", 0), service("worker", 0)],
      maxConcurrency: 2,
      failFast: true
    }
    const result = await run(input, okActivities())
    expect(result.kind).toBe("AllSucceeded")
    expect(result.perService).toHaveLength(3)
    expect(result.perService.every((entry) => entry.result.kind === "Succeeded")).toBe(true)
  })

  it("fail-fast: one service failing aborts the in-flight siblings (N4)", async () => {
    // A monitor that never returns on its own — the siblings sit in it until
    // aborted, while "api" fails its health check up front.
    const cancellableMonitor = async (): Promise<never> => {
      for (;;) {
        Context.current().heartbeat()
        await Context.current().sleep(50)
      }
    }
    const activities: DeploymentActivities = {
      ...okActivities(),
      healthCheck: async (p) => {
        if (p.service === "api") {
          throw ApplicationFailure.nonRetryable("probe 503", "HealthCheckFailed")
        }
      },
      monitorStep: cancellableMonitor
    }
    const input: MultiServiceInput = {
      services: [service("api", 600_000), service("web", 600_000), service("worker", 600_000)],
      maxConcurrency: 3,
      failFast: true
    }
    const result = await run(input, activities)

    expect(result.kind).toBe("SomeFailed")
    const byService = Object.fromEntries(result.perService.map((entry) => [entry.service, entry.result.kind]))
    expect(byService["api"]).toBe("Failed")
    // The siblings that were monitoring got aborted by the parent's fail-fast.
    expect(byService["web"]).toBe("Aborted")
    expect(byService["worker"]).toBe("Aborted")
  })
}, 120_000)
