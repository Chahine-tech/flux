import { fileURLToPath } from "node:url"
import { Context } from "@temporalio/activity"
import { ApplicationFailure } from "@temporalio/common"
import { TestWorkflowEnvironment } from "@temporalio/testing"
import { bundleWorkflowCode, Worker } from "@temporalio/worker"
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
  recordOutcome: async () => {},
  postmortem: async () => {}
})

let env: TestWorkflowEnvironment
let workflowBundle: Awaited<ReturnType<typeof bundleWorkflowCode>>

beforeAll(async () => {
  env = await TestWorkflowEnvironment.createTimeSkipping()
  // Bundle the workflow code once for the whole file, then reuse it for every
  // worker below. Passing `workflowsPath` to each `Worker.create` re-runs webpack
  // per test (~2s each) — slow, and enough to time out on a cold CI runner.
  workflowBundle = await bundleWorkflowCode({ workflowsPath })
  // The workflow upserts these, so they must exist on the ephemeral server too.
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

const run = async (
  activities: DeploymentActivities,
  input: DeploymentInput = baseInput
): Promise<DeploymentResult> => {
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
      workflowId: `wf-${Date.now()}-${Math.random().toString(36).slice(2)}`,
      args: [input]
    })
  ) as Promise<DeploymentResult>
}

/**
 * Like `run`, but also hands back the state the workflow reports once it is
 * over. Querying a closed workflow is allowed, and it is the only way to see
 * what an operator running `flux status` after the fact would be told.
 */
const runAndQuery = async (
  activities: DeploymentActivities,
  input: DeploymentInput = baseInput
): Promise<{ readonly result: DeploymentResult; readonly finalState: DeploymentState }> => {
  const worker = await Worker.create({
    connection: env.nativeConnection,
    namespace: env.namespace ?? "default",
    taskQueue: TASK_QUEUE,
    workflowBundle,
    activities
  })
  const workflowId = `wf-${Date.now()}-${Math.random().toString(36).slice(2)}`
  // Both inside `runUntil`: a query is answered by replaying the workflow on a
  // worker, so querying after the worker has shut down simply hangs. The same
  // shape as `FAILED_PRECONDITION: no poller seen for task queue recently`.
  return worker.runUntil(async () => {
    const result = await env.client.workflow.execute("deploymentWorkflow", {
      taskQueue: TASK_QUEUE,
      workflowId,
      args: [input]
    }) as DeploymentResult
    const finalState = await env.client.workflow.getHandle(workflowId).query<DeploymentState>("status")
    return { result, finalState }
  })
}

// Poll the workflow's `status` query until it reaches `phase`. Time-skipping
// fast-forwards the workflow's own timers, but reaching a queryable phase still
// takes a few real milliseconds, so we poll rather than assume a delay. Throws
// if the phase never arrives, which fails the test with a clear message.
const waitForPhase = async (
  query: () => Promise<DeploymentState>,
  phase: DeploymentState["phase"],
  tries = 100
): Promise<DeploymentState> => {
  for (let i = 0; i < tries; i++) {
    const state = await query()
    if (state.phase === phase) return state
    await new Promise((resolve) => setTimeout(resolve, 50))
  }
  throw new Error(`workflow never reached phase "${phase}"`)
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
      monitorStep: async () => (++step >= 2 ? { _tag: "Breached", breaches: [{ metric: "errorRate", observed: 0.05, limit: 0.01 }], action: "rollback" as const } : { _tag: "Within" })
    })
    expect(result.kind).toBe("RolledBack")
    if (result.kind === "RolledBack") {
      expect(result.atPercent).toBe(50)
      expect(result.toVersion).toBe("v2.0.8")
    }
  })

  it("schedules the rollback compensation at higher task-queue priority than forward shifts", async () => {
    let step = 0
    const worker = await Worker.create({
      connection: env.nativeConnection,
      namespace: env.namespace ?? "default",
      taskQueue: TASK_QUEUE,
      workflowBundle,
      activities: {
        ...okActivities(),
        monitorStep: async () =>
          (++step >= 2
            ? { _tag: "Breached", breaches: [{ metric: "errorRate", observed: 0.05, limit: 0.01 }], action: "rollback" as const }
            : { _tag: "Within" })
      }
    })
    const workflowId = `priority-${Date.now()}-${Math.random().toString(36).slice(2)}`
    await worker.runUntil(
      env.client.workflow.execute("deploymentWorkflow", { taskQueue: TASK_QUEUE, workflowId, args: [baseInput] })
    )

    const history = await env.client.workflow.getHandle(workflowId).fetchHistory()
    const shifts = (history.events ?? []).filter(
      (e) => e.activityTaskScheduledEventAttributes?.activityType?.name === "setTrafficWeight"
    )
    const priorities = shifts.map((e) => e.activityTaskScheduledEventAttributes?.priority?.priorityKey)
    // The compensation (restore previous version) is scheduled at priority 1;
    // the forward shifts run at the default, so under load the rollback wins.
    expect(priorities).toContain(1)
    expect(priorities.some((key) => key !== 1)).toBe(true)
  })

  it("escalates to RollbackFailed when the previous version is unhealthy after rollback", async () => {
    let step = 0
    const notifications: Array<string> = []
    const result = await run({
      ...okActivities(),
      // The new version passes its pre-shift probe; the *previous* version fails
      // the post-rollback verification, so the rollback did not restore health.
      healthCheck: async (params: { version: string }) => {
        if (params.version === "v2.0.8") throw new Error("previous version is down")
      },
      monitorStep: async () =>
        (++step >= 2
          ? { _tag: "Breached", breaches: [{ metric: "errorRate", observed: 0.05, limit: 0.01 }], action: "rollback" as const }
          : { _tag: "Within" }),
      notify: async (n: { kind: string }) => {
        notifications.push(n.kind)
      }
    })
    expect(result.kind).toBe("RollbackFailed")
    if (result.kind === "RollbackFailed") {
      expect(result.toVersion).toBe("v2.0.8")
      expect(result.version).toBe("v2.1.0")
      expect(result.atPercent).toBe(50)
      expect(result.reason).toContain("v2.0.8")
    }
    // The operator gets the loud notification, not the ordinary rolled-back one.
    expect(notifications).toContain("rollback-failed")
    expect(notifications).not.toContain("rolled-back")
  })

  // Blue/green: one flip to 100%, a bake, then success — or an instant
  // rollback on a breach. The same activities and ports as the canary; only the
  // shape of the rollout differs.
  const blueGreenInput: DeploymentInput = {
    service: "api",
    version: "v2.1.0",
    previousVersion: "v2.0.8",
    strategy: { kind: "blue-green", bakeMs: 0, requiresApproval: false },
    rules: [{ name: "errorRate", query: "q", max: 0.01 }],
    pollIntervalMs: 100
  }

  it("blue/green: flips to 100% and succeeds when the bake stays within budget", async () => {
    const shifts: Array<{ version: string; weight: number }> = []
    const result = await run({
      ...okActivities(),
      setTrafficWeight: async (p: { version: string; weight: number }) => {
        shifts.push({ version: p.version, weight: p.weight })
      }
    }, blueGreenInput)
    expect(result.kind).toBe("Succeeded")
    // A single atomic flip to the new version at 100% — no intermediate percentages.
    expect(shifts).toEqual([{ version: "v2.1.0", weight: 100 }])
  })

  it("runs a steps-only input (no strategy field) as canary, back-compat", async () => {
    // `baseInput` carries top-level `steps` and no `strategy`; the workflow must
    // normalize it to a canary, which is what keeps older histories replaying.
    expect(baseInput.strategy).toBeUndefined()
    const result = await run(okActivities(), baseInput)
    expect(result.kind).toBe("Succeeded")
  })

  it("blue/green: rolls back instantly when the bake breaches", async () => {
    const result = await run({
      ...okActivities(),
      monitorStep: async () => ({
        _tag: "Breached",
        breaches: [{ metric: "errorRate", observed: 0.05, limit: 0.01 }], action: "rollback" as const
      })
    }, blueGreenInput)
    expect(result.kind).toBe("RolledBack")
    if (result.kind === "RolledBack") {
      expect(result.atPercent).toBe(100)
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
      workflowBundle,
      activities: okActivities()
    })
    const result = (await worker.runUntil(async () => {
      const handle = await env.client.workflow.start("deploymentWorkflow", {
        taskQueue: TASK_QUEUE,
        workflowId: `wf-approve-${Date.now()}`,
        args: [input]
      })
      const parked = await waitForPhase(() => handle.query<DeploymentState>("status"), "awaiting-approval")
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
      workflowBundle,
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
      await waitForPhase(() => handle.query<DeploymentState>("status"), "awaiting-approval")
      await handle.executeUpdate("abort")
      return handle.result()
    })) as DeploymentResult
    expect(result.kind).toBe("Aborted")
    // The compensation restored the previous version to 100% traffic.
    expect(shifts.at(-1)).toEqual({ version: "v2.0.8", weight: 100 })
  })

  it("cancels an in-flight monitor on abort, instead of waiting out the window", async () => {
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
      workflowBundle,
      activities: { ...okActivities(), monitorStep: cancellableMonitor }
    })
    const result = (await worker.runUntil(async () => {
      const handle = await env.client.workflow.start("deploymentWorkflow", {
        taskQueue: TASK_QUEUE,
        workflowId: `wf-cancel-${Date.now()}`,
        args: [input]
      })
      await waitForPhase(() => handle.query<DeploymentState>("status"), "monitoring")
      await handle.executeUpdate("abort")
      return handle.result()
    })) as DeploymentResult
    expect(result.kind).toBe("Aborted")
    if (result.kind === "Aborted") {
      expect(result.atPercent).toBe(50)
    }
  })

  it("continues-as-new mid-rollout and still completes every step", async () => {
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
      workflowBundle,
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

/**
 * What the workflow says about itself once it is over.
 *
 * A k3d run (D50) showed the reported state outliving the traffic: a canary
 * rolled back to the previous version still answered `currentPercent: 10`,
 * because the saga restored the router without touching the state it reports.
 * An operator reading `flux status` afterwards would conclude a tenth of
 * production was still on the version that had just failed.
 */
describe("the state a finished deployment reports", () => {
  it("says nothing is on the new version once the rollback restored traffic", async () => {
    let step = 0
    const { finalState, result } = await runAndQuery({
      ...okActivities(),
      monitorStep: async () =>
        (++step >= 2
          ? { _tag: "Breached", breaches: [{ metric: "errorRate", observed: 0.05, limit: 0.01 }], action: "rollback" as const }
          : { _tag: "Within" })
    })
    expect(result.kind).toBe("RolledBack")
    expect(finalState.phase).toBe("done")
    // The number that matters, and the one that used to be the last percentage
    // attempted rather than the percentage in effect.
    expect(finalState.currentPercent).toBe(0)
  })

  it("keeps the percentage when the undo itself failed, because traffic may really be stranded", async () => {
    // The one case where the old value was the true one. A compensation that
    // throws leaves the router wherever it was, so reporting 0 here would be
    // the same lie in the opposite direction, and on the more dangerous side.
    let step = 0
    const { finalState } = await runAndQuery({
      ...okActivities(),
      monitorStep: async () =>
        (++step >= 2
          ? { _tag: "Breached", breaches: [{ metric: "errorRate", observed: 0.05, limit: 0.01 }], action: "rollback" as const }
          : { _tag: "Within" }),
      // `setTrafficWeight` serves both directions, so failing it outright would
      // break the first forward shift and the workflow would never reach a
      // rollback at all. Only the undo fails: the previous version back at 100%.
      setTrafficWeight: async (p: { readonly version: string; readonly weight: number }) => {
        if (p.version === baseInput.previousVersion && p.weight === 100) {
          throw new Error("router unreachable")
        }
      }
    } as unknown as DeploymentActivities)
    expect(finalState.currentPercent).toBeGreaterThan(0)
  })
})
