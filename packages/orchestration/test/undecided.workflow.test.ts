import { fileURLToPath } from "node:url"
import { TestWorkflowEnvironment } from "@temporalio/testing"
import { bundleWorkflowCode, Worker } from "@temporalio/worker"
import { afterAll, beforeAll, describe, expect, it } from "vitest"
import type { DeploymentActivities } from "../src/activities/types.ts"
import { type DeploymentInput, type DeploymentResult, SEARCH_ATTRIBUTES } from "../src/deployment-input.ts"

/**
 * What the workflow does when the readings cannot decide.
 *
 * A rule carrying `sampleSize` can come back `Inconclusive`: the window ended
 * without enough observations to tell a healthy version from a failing one.
 * Promoting on that is a coin flip and rolling back punishes a version that did
 * nothing wrong, so the workflow keeps watching, up to `maxMonitorMs`, and
 * rolls back only once the budget is spent.
 */

const TASK_QUEUE = "flux-undecided-test"
const workflowsPath = fileURLToPath(new URL("../src/workflows/index.ts", import.meta.url))

const undecided = {
  _tag: "Inconclusive" as const,
  pending: [{
    metric: "taskFailureRate",
    observed: 0.033,
    limit: 0.05,
    sampleSize: 30,
    lower: 0.006,
    upper: 0.167
  }] as const
}

const input = (over: Partial<DeploymentInput> = {}): DeploymentInput => ({
  service: "agent",
  version: "v2",
  previousVersion: "v1",
  steps: [{ percent: 10, monitorMs: 1_000, requiresApproval: false }],
  rules: [{ name: "taskFailureRate", query: "q", max: 0.05, sampleSize: "n" }],
  pollIntervalMs: 100,
  ...over
})

let env: TestWorkflowEnvironment
let workflowBundle: Awaited<ReturnType<typeof bundleWorkflowCode>>

beforeAll(async () => {
  env = await TestWorkflowEnvironment.createTimeSkipping()
  workflowBundle = await bundleWorkflowCode({ workflowsPath })
  // Without these the workflow's `upsertSearchAttributes` fails on every
  // activation and the execution never finishes, which reads as a hung test
  // rather than as the setup gap it is.
  await env.connection.operatorService.addSearchAttributes({
    namespace: env.namespace ?? "default",
    searchAttributes: {
      // temporal.api.enums.v1.IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD
      [SEARCH_ATTRIBUTES.service]: 2,
      [SEARCH_ATTRIBUTES.version]: 2,
      [SEARCH_ATTRIBUTES.status]: 2
    }
  }).catch((error: unknown) => {
    if (!/already exist/i.test(String(error))) throw error
  })
}, 120_000)

afterAll(async () => {
  await env?.teardown()
})

/** Counts monitor calls, so "it kept watching" is measured rather than assumed. */
const harness = (verdicts: ReadonlyArray<unknown>) => {
  let calls = 0
  const activities: DeploymentActivities = {
    healthCheck: async () => {},
    setTrafficWeight: async () => {},
    monitorStep: async () => {
      const verdict = verdicts[Math.min(calls, verdicts.length - 1)]
      calls++
      return verdict as Awaited<ReturnType<DeploymentActivities["monitorStep"]>>
    },
    notify: async () => {},
    readRouterState: async () => [],
    recordOutcome: async () => {},
    postmortem: async () => {}
  }
  return { activities, monitorCalls: () => calls }
}

const run = async (activities: DeploymentActivities, deployment: DeploymentInput): Promise<DeploymentResult> => {
  const worker = await Worker.create({
    connection: env.nativeConnection,
    namespace: env.namespace ?? "default",
    taskQueue: TASK_QUEUE,
    workflowBundle,
    activities
  })
  return worker.runUntil(
    env.client.workflow.execute("deploymentWorkflow", {
      taskQueue: TASK_QUEUE,
      workflowId: `undecided-${Date.now()}-${Math.random().toString(36).slice(2)}`,
      args: [deployment]
    })
  ) as Promise<DeploymentResult>
}

describe("a window that cannot decide", () => {
  it("keeps watching until the budget is spent, then rolls back", async () => {
    const { activities, monitorCalls } = harness([undecided])
    const result = await run(activities, input({ maxMonitorMs: 3_000 }))

    // Three windows of 1s inside a 3s budget, and only then a rollback: the
    // version is not promoted on a sample that proves nothing, and not punished
    // before it has been given the time to prove something.
    expect(monitorCalls()).toBe(3)
    expect(result.kind).toBe("RolledBack")
  }, 60_000)

  it("stops watching the moment the evidence arrives", async () => {
    const { activities, monitorCalls } = harness([undecided, undecided, { _tag: "Within" }])
    const result = await run(activities, input({ maxMonitorMs: 10_000 }))

    // The budget allows ten windows; it used three, because the third decided.
    expect(monitorCalls()).toBe(3)
    expect(result.kind).toBe("Succeeded")
  }, 60_000)

  it("rolls back after a single window when no budget was given", async () => {
    // The historical shape: one window, one verdict, no extension.
    const { activities, monitorCalls } = harness([undecided])
    const result = await run(activities, input())

    expect(monitorCalls()).toBe(1)
    expect(result.kind).toBe("RolledBack")
  }, 60_000)

  it("still rolls back immediately on a real breach, budget or not", async () => {
    const breach = { _tag: "Breached" as const, breaches: [{ metric: "taskFailureRate", observed: 0.4, limit: 0.05 }], action: "rollback" as const }
    const { activities, monitorCalls } = harness([breach])
    const result = await run(activities, input({ maxMonitorMs: 10_000 }))

    expect(monitorCalls()).toBe(1)
    expect(result.kind).toBe("RolledBack")
  }, 60_000)

  it("does not hang when the window is zero and the verdict never settles", async () => {
    // The loop's termination depends on `spentMs` growing. A zero window grows
    // it by nothing, so without the guard this runs forever, and in a durable
    // workflow "forever" survives restarts.
    const { activities, monitorCalls } = harness([undecided])
    const result = await run(activities, input({
      steps: [{ percent: 10, monitorMs: 0, requiresApproval: false }],
      maxMonitorMs: 5_000
    }))

    expect(monitorCalls()).toBe(1)
    expect(result.kind).toBe("RolledBack")
  }, 60_000)
})

describe("verdicts signalled into a running deployment", () => {
  it("counts them and hands the running tally to each window", async () => {
    // One verdict arrives per window, from inside the activity, which is the
    // shape that matters: a verdict that lands mid-deployment has to reach the
    // *next* decision, not the one already taken.
    const seen: Array<{ total: number; failures: number } | undefined> = []
    let signal: ((success: boolean) => Promise<void>) | undefined

    const activities: DeploymentActivities = {
      healthCheck: async () => {},
      setTrafficWeight: async () => {},
      monitorStep: async (params) => {
        seen.push(params.outcomes)
        await signal?.(true)
        return undecided as Awaited<ReturnType<DeploymentActivities["monitorStep"]>>
      },
      notify: async () => {},
      readRouterState: async () => [],
      recordOutcome: async () => {},
      postmortem: async () => {}
    }

    const worker = await Worker.create({
      connection: env.nativeConnection,
      namespace: env.namespace ?? "default",
      taskQueue: TASK_QUEUE,
      workflowBundle,
      activities
    })
    const handle = await env.client.workflow.start("deploymentWorkflow", {
      taskQueue: TASK_QUEUE,
      workflowId: `outcomes-${Date.now()}-${Math.random().toString(36).slice(2)}`,
      args: [input({
        maxMonitorMs: 3_000,
        outcomeRule: { name: "taskFailureRate", max: 0.05 }
      })]
    })
    signal = (success) => handle.signal("taskOutcome", { version: "v2", success })

    await worker.runUntil(handle.result())

    // Three windows inside the budget, each seeing one more verdict than the
    // last. The first sees none, because none had been sent yet.
    expect(seen).toEqual([
      { total: 0, failures: 0 },
      { total: 1, failures: 0 },
      { total: 2, failures: 0 }
    ])
  }, 60_000)

  it("ignores verdicts about a version it is not rolling out", async () => {
    // The rule is a limit on the new version, not a comparison between two.
    const seen: Array<{ total: number; failures: number } | undefined> = []
    let signal: ((version: string) => Promise<void>) | undefined

    const activities: DeploymentActivities = {
      healthCheck: async () => {},
      setTrafficWeight: async () => {},
      monitorStep: async (params) => {
        seen.push(params.outcomes)
        await signal?.("v1")
        return undecided as Awaited<ReturnType<DeploymentActivities["monitorStep"]>>
      },
      notify: async () => {},
      readRouterState: async () => [],
      recordOutcome: async () => {},
      postmortem: async () => {}
    }

    const worker = await Worker.create({
      connection: env.nativeConnection,
      namespace: env.namespace ?? "default",
      taskQueue: TASK_QUEUE,
      workflowBundle,
      activities
    })
    const handle = await env.client.workflow.start("deploymentWorkflow", {
      taskQueue: TASK_QUEUE,
      workflowId: `other-version-${Date.now()}-${Math.random().toString(36).slice(2)}`,
      args: [input({ maxMonitorMs: 2_000, outcomeRule: { name: "taskFailureRate", max: 0.05 } })]
    })
    signal = (version) => handle.signal("taskOutcome", { version, success: false })

    await worker.runUntil(handle.result())

    expect(seen.every((tally) => tally?.total === 0)).toBe(true)
  }, 60_000)
})

describe("a breach that is a tradeoff rather than a fault", () => {
  // The motivating case: a version better on every technical measure that costs
  // 38% more per unit of work has not regressed, it has presented a bill.
  const tradeoff = {
    _tag: "Breached" as const,
    breaches: [{ metric: "costPerTask", observed: 0.138, limit: 0.10 }],
    action: "pause" as const
  }

  /** Drives one deployment, reacting once the workflow reports it is paused. */
  const runPaused = async (
    verdict: unknown,
    react: ((handle: { signal: Function; query: Function }) => Promise<void>) | undefined,
    over: Partial<DeploymentInput> = {}
  ) => {
    const phases: Array<string> = []
    const activities: DeploymentActivities = {
      healthCheck: async () => {},
      setTrafficWeight: async () => {},
      monitorStep: async () => verdict as Awaited<ReturnType<DeploymentActivities["monitorStep"]>>,
      notify: async () => {},
      readRouterState: async () => [],
      recordOutcome: async () => {},
      postmortem: async () => {}
    }
    const worker = await Worker.create({
      connection: env.nativeConnection,
      namespace: env.namespace ?? "default",
      taskQueue: TASK_QUEUE,
      workflowBundle,
      activities
    })
    const handle = await env.client.workflow.start("deploymentWorkflow", {
      taskQueue: TASK_QUEUE,
      workflowId: `pause-${Date.now()}-${Math.random().toString(36).slice(2)}`,
      args: [input(over)]
    })

    const result = await worker.runUntil(async () => {
      if (react !== undefined) {
        // Wait for the gate to actually be open before acting on it.
        for (let i = 0; i < 100; i++) {
          const state = await handle.query("status") as { phase: string }
          phases.push(state.phase)
          if (state.phase === "paused") break
          await new Promise((resolve) => setTimeout(resolve, 100))
        }
        await react(handle as never)
      }
      return handle.result()
    }) as DeploymentResult

    return { result, sawPaused: phases.includes("paused") }
  }

  it("stops, reports itself paused, and resumes when the tradeoff is accepted", async () => {
    const { result, sawPaused } = await runPaused(tradeoff, async (handle) => {
      await (handle as unknown as { executeUpdate: Function }).executeUpdate("approve")
    })
    expect(sawPaused).toBe(true)
    // Traffic was never unwound: accepting the bill promotes the version.
    expect(result.kind).toBe("Succeeded")
  }, 90_000)

  it("unwinds when the tradeoff is refused", async () => {
    const { result } = await runPaused(tradeoff, async (handle) => {
      await (handle as unknown as { executeUpdate: Function }).executeUpdate("abort")
    })
    expect(result.kind).toBe("Aborted")
  }, 90_000)

  it("rolls back when nobody answers within the pause window", async () => {
    // A tradeoff nobody accepted has not been accepted.
    const { result } = await runPaused(tradeoff, undefined, { pauseTimeoutMs: 2_000 })
    expect(result.kind).toBe("RolledBack")
  }, 90_000)
})
