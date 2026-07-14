import { Effect } from "effect"
import { TestWorkflowEnvironment } from "@temporalio/testing"
import { Worker } from "@temporalio/worker"
import { SEARCH_ATTRIBUTES } from "@flux/orchestration"
import type { DeploymentActivities } from "@flux/orchestration/activities"
import { afterAll, beforeAll, describe, expect, it } from "vitest"
import { fileURLToPath } from "node:url"
import { make } from "../src/temporal-client.ts"

/**
 * The real chain, end to end: the control plane's own `TemporalClient` drives a
 * real deployment on a real (time-skipping) Temporal server, running the real
 * workflow bundle. This is the seam the isolated unit tests can't cover — the
 * production `start` / `status` / `approve` / `abort` code exercised against a
 * live server, not a mock — proving trigger → status progression → decision →
 * terminal outcome actually works together.
 *
 * Activities are stubbed (health OK, metrics within budget, router/notify no-op)
 * because this asserts the orchestration + control-plane wiring, not the
 * adapters, so no target service or router is needed. The visibility-backed
 * methods (`list` / `listClosed` / `/stats`) are covered elsewhere — the
 * time-skipping server does not implement `ListWorkflowExecutions`, and `/stats`
 * is verified live against a real Temporal.
 */

const KEYWORD = 2 // IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD
const TASK_QUEUE = "flux-deployments"
const workflowsPath = fileURLToPath(import.meta.resolve("@flux/orchestration/workflows"))

const okActivities = (): DeploymentActivities => ({
  healthCheck: async () => {},
  setTrafficWeight: async () => {},
  monitorStep: async () => ({ _tag: "Within" }),
  notify: async () => {},
  readRouterState: async () => [],
  recordOutcome: async () => {}
})

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

const gatedRequest = {
  service: "api",
  version: "v2",
  previousVersion: "v1",
  steps: [
    { percent: 50, monitorMs: 0, requiresApproval: true },
    { percent: 100, monitorMs: 0, requiresApproval: false }
  ],
  rules: [{ name: "errorRate", query: "q", max: 0.01 }],
  pollIntervalMs: 100
} as const

let env: TestWorkflowEnvironment

beforeAll(async () => {
  env = await TestWorkflowEnvironment.createTimeSkipping()
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

/**
 * Run `drive` while a worker processes the task queue. `drive` uses the real
 * control-plane `TemporalClient` (`make(env.client)`) to start a deployment,
 * wait for it to park at the approval gate, then take the given decision;
 * returns the terminal `DeploymentResult`.
 */
const runWithDecision = async (
  decide: (temporal: ReturnType<typeof make>, workflowId: string) => Promise<void>
): Promise<{ readonly kind: string }> => {
  const temporal = make(env.client)
  const worker = await Worker.create({
    connection: env.nativeConnection,
    namespace: env.namespace ?? "default",
    taskQueue: TASK_QUEUE,
    workflowsPath,
    activities: okActivities()
  })

  return worker.runUntil(async () => {
    const workflowId = await Effect.runPromise(temporal.start(gatedRequest))

    let parkedPercent = -1
    for (let i = 0; i < 100; i++) {
      const state = await Effect.runPromise(temporal.status(workflowId))
      if (state.phase === "awaiting-approval") {
        parkedPercent = state.currentPercent
        break
      }
      await sleep(50)
    }
    expect(parkedPercent).toBe(50)

    await decide(temporal, workflowId)
    return env.client.workflow.getHandle(workflowId).result()
  }) as Promise<{ readonly kind: string }>
}

describe("control plane e2e", () => {
  it("trigger -> status parks at gate -> approve -> Succeeded", async () => {
    const result = await runWithDecision((temporal, workflowId) => Effect.runPromise(temporal.approve(workflowId)))
    expect(result.kind).toBe("Succeeded")
  }, 90_000)

  it("trigger -> status parks at gate -> abort -> Aborted", async () => {
    const result = await runWithDecision((temporal, workflowId) => Effect.runPromise(temporal.abort(workflowId)))
    expect(result.kind).toBe("Aborted")
  }, 90_000)
})
