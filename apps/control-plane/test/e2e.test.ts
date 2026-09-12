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
  recordOutcome: async () => {},
  postmortem: async () => {}
})

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

const gatedRequest = {
  service: "api",
  version: "v2",
  previousVersion: "v1",
  strategy: {
    kind: "canary",
    steps: [
      { percent: 50, monitorMs: 0, requiresApproval: true },
      { percent: 100, monitorMs: 0, requiresApproval: false }
    ]
  },
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
  it("reports the namespace reachable, against a real server", async () => {
    // The health endpoint's own tests stub this; here the real
    // `describeNamespace` call runs. Worth its own test because the wire value
    // is a numeric enum and `JSON.stringify` renders it as a name — an early
    // version compared against the string and reported a healthy cluster as
    // not ready, which no stub would have caught. The *negative* case lives in
    // `real-schedules.test.ts` instead: the time-skipping server registers a
    // namespace on demand, so an absent one comes back healthy here.
    expect(await Effect.runPromise(make(env.client).reachable)).toBe(true)
  })

  it("stamps the service as the deployment's fairness key", async () => {
    const temporal = make(env.client)
    // No worker needed: the start event is recorded whether or not anything
    // picks the workflow up, and that event is the only place the key appears.
    const workflowId = await Effect.runPromise(temporal.start({ ...gatedRequest, service: "checkout" }))

    const history = await env.client.workflow.getHandle(workflowId).fetchHistory()
    const started = history.events?.[0]?.workflowExecutionStartedEventAttributes
    expect(started?.priority?.fairnessKey).toBe("checkout")

    // The activities carry no priority of their own: an absent field means
    // "inherit from the calling workflow", so the key is proven where it is
    // set, not where it is used. D35's rollback priority is the opposite case
    // — an explicit override, which does materialise on the activity.
    const withPriority = (history.events ?? []).filter((event) =>
      event.activityTaskScheduledEventAttributes?.priority !== undefined &&
      event.activityTaskScheduledEventAttributes?.priority !== null
    )
    expect(withPriority).toHaveLength(0)
  })

  it("trigger -> status parks at gate -> approve -> Succeeded", async () => {
    const result = await runWithDecision((temporal, workflowId) => Effect.runPromise(temporal.approve(workflowId)))
    expect(result.kind).toBe("Succeeded")
  }, 90_000)

  it("trigger -> status parks at gate -> abort -> Aborted", async () => {
    const result = await runWithDecision((temporal, workflowId) => Effect.runPromise(temporal.abort(workflowId)))
    expect(result.kind).toBe("Aborted")
  }, 90_000)
})
