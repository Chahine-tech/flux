import { fileURLToPath } from "node:url"
import { Client, Connection } from "@temporalio/client"
import { bundleWorkflowCode, NativeConnection, Worker } from "@temporalio/worker"
import type { DeploymentActivities, DeploymentInput, DeploymentResult } from "@flux/orchestration"
import { DeployServiceHandler } from "@flux/orchestration"
import { afterAll, beforeAll, describe, expect, it } from "vitest"
import { ensureSearchAttributes } from "../src/search-attributes.ts"

/**
 * flux-as-a-service (N9/D25): a caller namespace triggers a canary in a
 * separate platform namespace through a Nexus endpoint, with no access to
 * that namespace otherwise — the multi-tenant story that makes Nexus
 * genuine. The time-skipping test server does not implement Nexus, so this
 * runs against the real cluster only, same gate as D19:
 *
 *   docker compose up -d postgresql temporal
 *   FLUX_REAL_TEMPORAL=1 pnpm --filter @flux/worker test
 *
 * Namespaces and the Nexus endpoint are created idempotently here rather
 * than as a manual one-off step, matching `ensureSearchAttributes` — the
 * test owns the infra it needs, the same as the app would.
 */
const REAL = process.env.FLUX_REAL_TEMPORAL === "1"
const address = process.env.TEMPORAL_ADDRESS ?? "localhost:7233"

const PLATFORM_NAMESPACE = "flux-platform"
const CALLER_NAMESPACE = "flux-team-a"
const ENDPOINT_NAME = "flux-deploy"
const PLATFORM_TASK_QUEUE = "flux-nexus-deploy"

const workflowsPath = fileURLToPath(import.meta.resolve("@flux/orchestration/workflows"))

const okActivities = (): DeploymentActivities => ({
  healthCheck: async () => {},
  setTrafficWeight: async () => {},
  monitorStep: async () => ({ _tag: "Within" }),
  notify: async () => {},
  readRouterState: async () => [],
  recordOutcome: async () => {}
})

// Namespace registration says "already exists"; Nexus endpoint creation says
// "already registered" — different APIs, different wording, found empirically.
const ignoreAlreadyExists = (error: unknown) => {
  const message = error instanceof Error ? error.message : String(error)
  if (!/already (exist|registered)/i.test(message)) throw error
}

let platformConnection: NativeConnection
let callerConnection: NativeConnection
let platformClient: Client
let callerClient: Client
let workflowBundle: Awaited<ReturnType<typeof bundleWorkflowCode>>

beforeAll(async () => {
  if (!REAL) return

  const setupConnection = await Connection.connect({ address })
  try {
    await setupConnection.workflowService.registerNamespace({
      namespace: PLATFORM_NAMESPACE,
      // protobuf.js accepts a plain number at runtime (verified); the
      // generated `IDuration.seconds` type is `Long | null` only.
      workflowExecutionRetentionPeriod: { seconds: 86_400 as unknown as never }
    }).catch(ignoreAlreadyExists)
    await setupConnection.workflowService.registerNamespace({
      namespace: CALLER_NAMESPACE,
      // protobuf.js accepts a plain number at runtime (verified); the
      // generated `IDuration.seconds` type is `Long | null` only.
      workflowExecutionRetentionPeriod: { seconds: 86_400 as unknown as never }
    }).catch(ignoreAlreadyExists)
    await setupConnection.operatorService.createNexusEndpoint({
      spec: {
        name: ENDPOINT_NAME,
        target: { worker: { namespace: PLATFORM_NAMESPACE, taskQueue: PLATFORM_TASK_QUEUE } }
      }
    }).catch(ignoreAlreadyExists)
  } finally {
    await setupConnection.close()
  }

  await ensureSearchAttributes(address, PLATFORM_NAMESPACE)
  workflowBundle = await bundleWorkflowCode({ workflowsPath })

  platformConnection = await NativeConnection.connect({ address })
  callerConnection = await NativeConnection.connect({ address })
  platformClient = new Client({ connection: await Connection.connect({ address }), namespace: PLATFORM_NAMESPACE })
  callerClient = new Client({ connection: await Connection.connect({ address }), namespace: CALLER_NAMESPACE })
}, 120_000)

afterAll(async () => {
  await platformConnection?.close()
  await callerConnection?.close()
})

describe.skipIf(!REAL)("flux-as-a-service via Temporal Nexus (N9/D25)", () => {
  it("a canary triggered from the caller namespace completes through the platform namespace", async () => {
    // The platform worker: the same `deploymentWorkflow` + activities any
    // direct caller runs, plus the Nexus service that exposes it. Nexus tasks
    // and the workflow it starts share this worker's task queue (D25).
    const platformWorker = await Worker.create({
      connection: platformConnection,
      namespace: PLATFORM_NAMESPACE,
      taskQueue: PLATFORM_TASK_QUEUE,
      workflowBundle,
      activities: okActivities(),
      nexusServices: [DeployServiceHandler]
    })

    // The caller worker: only the caller workflow, in its own namespace, with
    // no activities and no access to the platform namespace's task queue.
    const callerTaskQueue = `flux-team-a-tasks-${Date.now()}`
    const callerWorker = await Worker.create({
      connection: callerConnection,
      namespace: CALLER_NAMESPACE,
      taskQueue: callerTaskQueue,
      workflowBundle,
      activities: {}
    })

    const input: DeploymentInput = {
      service: "checkout",
      version: "v2",
      previousVersion: "v1",
      steps: [{ percent: 100, monitorMs: 0, requiresApproval: false }],
      rules: [{ name: "errorRate", query: "q", max: 0.01 }],
      pollIntervalMs: 50
    }

    // The platform worker is a passive Nexus target with nothing of its own to
    // await — run it in the background for the caller's whole execution,
    // then shut it down explicitly (unlike `runUntil`, which only fits a
    // worker that's driving its own action).
    const platformRun = platformWorker.run()
    let result: DeploymentResult
    try {
      result = await callerWorker.runUntil(() =>
        callerClient.workflow.execute("nexusCallerWorkflow", {
          taskQueue: callerTaskQueue,
          workflowId: `nexus-caller-${Date.now()}`,
          args: [input]
        })) as DeploymentResult
    } finally {
      platformWorker.shutdown()
      await platformRun
    }

    expect(result.kind).toBe("Succeeded")

    // The workflow the caller actually observed ran in the platform namespace,
    // under the Nexus-derived workflow id — proving the cross-namespace hop,
    // not just that some workflow somewhere completed.
    const platformExecutions: Array<string> = []
    for await (
      const execution of platformClient.workflow.list({ query: "WorkflowType = 'deploymentWorkflow'" })
    ) {
      platformExecutions.push(execution.workflowId)
    }
    expect(platformExecutions.some((id) => id.startsWith("nexus-deploy-checkout-"))).toBe(true)
  }, 60_000)
}, 120_000)
