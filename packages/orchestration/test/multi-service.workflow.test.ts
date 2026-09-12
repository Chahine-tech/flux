import { fileURLToPath } from "node:url"
import { Context } from "@temporalio/activity"
import { ApplicationFailure } from "@temporalio/common"
import { TestWorkflowEnvironment } from "@temporalio/testing"
import { bundleWorkflowCode, Worker } from "@temporalio/worker"
import { afterAll, beforeAll, describe, expect, it } from "vitest"
import { compileRolloutPlan } from "@flux/domain"
import type { DeploymentActivities } from "../src/activities/types.ts"
import { type DeploymentInput, type MultiServiceInput, type MultiServiceResult, SEARCH_ATTRIBUTES } from "../src/deployment-input.ts"

/**
 * Multi-service parent workflow against a time-skipping Temporal server:
 * it starts one real `deploymentWorkflow` child per service and coordinates them.
 * Activities are mocked so we test the parent's scheduling and failure
 * policies: fan-out, fail-fast, dependency ordering, and abort-dependents.
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
  recordOutcome: async () => {},
  postmortem: async () => {}
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

/** The id of the most recent rollout, so a test can read its recorded history. */
let lastRolloutId = ""

const run = async (input: MultiServiceInput, activities: DeploymentActivities): Promise<MultiServiceResult> => {
  const worker = await Worker.create({
    connection: env.nativeConnection,
    namespace: env.namespace ?? "default",
    taskQueue: TASK_QUEUE,
    workflowBundle,
    activities
  })
  lastRolloutId = `multi-${Date.now()}-${Math.random().toString(36).slice(2)}`
  return worker.runUntil(
    env.client.workflow.execute("multiServiceDeployment", {
      taskQueue: TASK_QUEUE,
      workflowId: lastRolloutId,
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

  it("fail-fast: one service failing aborts the in-flight siblings", async () => {
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

  it("deploys a chain in dependency order", async () => {
    // The plan is compiled here, exactly as the control plane does before
    // starting the workflow — the graph never crosses into the parent.
    const compiled = compileRolloutPlan(["web", "api", "db"], { web: ["api"], api: ["db"] })
    expect(compiled._tag).toBe("Compiled")
    if (compiled._tag !== "Compiled") return

    const shifted: Array<string> = []
    const activities: DeploymentActivities = {
      ...okActivities(),
      setTrafficWeight: async (p) => {
        shifted.push(p.service)
      }
    }

    const result = await run({
      // Declared in reverse on purpose: the order that matters is the plan's.
      services: [service("web", 0), service("api", 0), service("db", 0)],
      maxConcurrency: 3,
      failFast: false,
      onFailure: "abort-dependents",
      plan: compiled.plan
    }, activities)

    expect(result.kind).toBe("AllSucceeded")
    // Even with three slots free, nothing overtakes its dependency.
    expect(shifted).toEqual(["db", "api", "web"])
  })

  it("abort-dependents: a failure stops what depends on it and spares the rest", async () => {
    //   db ──> api ──┐
    //     └──> cache ─┴──> web
    // api fails, so web is blocked; cache shares only db and must finish.
    const compiled = compileRolloutPlan(["db", "api", "cache", "web"], {
      api: ["db"],
      cache: ["db"],
      web: ["api", "cache"]
    })
    expect(compiled._tag).toBe("Compiled")
    if (compiled._tag !== "Compiled") return

    const activities: DeploymentActivities = {
      ...okActivities(),
      healthCheck: async (p) => {
        if (p.service === "api") {
          throw ApplicationFailure.nonRetryable("probe 503", "HealthCheckFailed")
        }
      }
    }

    const result = await run({
      services: [service("db", 0), service("api", 0), service("cache", 0), service("web", 0)],
      maxConcurrency: 4,
      failFast: false,
      onFailure: "abort-dependents",
      plan: compiled.plan
    }, activities)

    expect(result.kind).toBe("SomeFailed")
    const byService = Object.fromEntries(result.perService.map((entry) => [entry.service, entry.result]))

    expect(byService["db"]!.kind).toBe("Succeeded")
    expect(byService["api"]!.kind).toBe("Failed")
    // The independent branch is untouched — this is the whole point of the policy.
    expect(byService["cache"]!.kind).toBe("Succeeded")
    // web never started, so it is Skipped rather than Failed, and says why.
    expect(byService["web"]).toEqual({ kind: "Skipped", service: "web", blockedBy: "api" })
  })

  it("gives each child its own fairness key, so one rollout cannot own the queue", async () => {
    await run({
      services: [service("api", 0), service("web", 0)],
      maxConcurrency: 2,
      failFast: false
    }, okActivities())

    const history = await env.client.workflow.getHandle(lastRolloutId).fetchHistory()
    const started = (history.events ?? []).flatMap((event) => {
      const attributes = event.startChildWorkflowExecutionInitiatedEventAttributes
      return attributes === undefined || attributes === null
        ? []
        : [{ workflowId: attributes.workflowId, fairnessKey: attributes.priority?.fairnessKey }]
    })

    expect(started).toHaveLength(2)
    // Keyed by service, not by rollout: two services are two tenants on the
    // queue, so a big rollout shares it instead of queueing ahead of an
    // unrelated deployment.
    const keys = started.map((entry) => entry.fairnessKey).sort()
    expect(keys).toEqual(["api", "web"])
    // And the key belongs to the child it was started for.
    for (const entry of started) {
      expect(entry.workflowId).toContain(entry.fairnessKey!)
    }
  })
}, 120_000)
