import { fileURLToPath } from "node:url"
import { Client, Connection } from "@temporalio/client"
import { bundleWorkflowCode, NativeConnection, Worker } from "@temporalio/worker"
import type { DeploymentActivities, DeploymentInput, DeploymentResult } from "@flux/orchestration"
import { afterAll, beforeAll, describe, expect, it } from "vitest"
import { ensureSearchAttributes } from "../src/search-attributes.ts"
import { tuner, versioningOptions } from "../src/worker-config.ts"

/**
 * Real-cluster proofs (D19): the capabilities the time-skipping test server
 * does not implement — Worker Deployment Versioning and the resource tuner —
 * run here against the compose's actual Temporal. Gated by FLUX_REAL_TEMPORAL=1
 * so the default `pnpm test` stays hermetic:
 *
 *   docker compose up -d postgresql temporal
 *   FLUX_REAL_TEMPORAL=1 pnpm --filter @flux/worker test
 *
 * Activities are mocked — the system under test is the worker/server contract,
 * not the adapters (those have their own integration test).
 */
const REAL = process.env.FLUX_REAL_TEMPORAL === "1"
const address = process.env.TEMPORAL_ADDRESS ?? "localhost:7233"
const namespace = process.env.TEMPORAL_NAMESPACE ?? "default"

const workflowsPath = fileURLToPath(import.meta.resolve("@flux/orchestration/workflows"))

const quickCanary = (service: string): DeploymentInput => ({
  service,
  version: "v2",
  previousVersion: "v1",
  steps: [{ percent: 100, monitorMs: 0, requiresApproval: false }],
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

let connection: NativeConnection
let client: Client
let workflowBundle: Awaited<ReturnType<typeof bundleWorkflowCode>>

beforeAll(async () => {
  if (!REAL) return
  await ensureSearchAttributes(address, namespace)
  workflowBundle = await bundleWorkflowCode({ workflowsPath })
  connection = await NativeConnection.connect({ address })
  client = new Client({ connection: await Connection.connect({ address }), namespace })
}, 120_000)

afterAll(async () => {
  await connection?.close()
})

describe.skipIf(!REAL)("real cluster (D19)", () => {
  it("resource tuner: a worker running the production tuner completes a canary", async () => {
    const taskQueue = `flux-real-tuner-${Date.now()}`
    const worker = await Worker.create({
      connection,
      namespace,
      taskQueue,
      workflowBundle,
      activities: okActivities(),
      tuner // the exact production value — this run would have caught "50 millis"
    })
    const result = await worker.runUntil(
      client.workflow.execute("deploymentWorkflow", {
        taskQueue,
        workflowId: `real-tuner-${Date.now()}`,
        args: [quickCanary("api")]
      })
    ) as DeploymentResult
    expect(result.kind).toBe("Succeeded")
  })

  it("worker versioning: a versioned worker pins the workflows it runs (D15)", async () => {
    const taskQueue = `flux-real-versioning-${Date.now()}`
    const deploymentName = `flux-worker-test-${Date.now()}`
    const buildId = "1.0.0"
    // Through the production code path: the same env-driven options main.ts uses.
    const options = versioningOptions({ FLUX_WORKER_BUILD_ID: buildId, FLUX_WORKER_DEPLOYMENT: deploymentName })
    expect(options).toBeDefined()

    const worker = await Worker.create({
      connection,
      namespace,
      taskQueue,
      workflowBundle,
      activities: okActivities(),
      workerDeploymentOptions: options!
    })

    const workflowId = `real-versioning-${Date.now()}`
    const result = await worker.runUntil(async () => {
      // A fresh deployment version receives default-routed tasks only once it is
      // the deployment's *current* version; the server learns about the version
      // from the worker's first poll, so retry until it is registered.
      for (let i = 0; ; i++) {
        try {
          await client.connection.workflowService.setWorkerDeploymentCurrentVersion({
            namespace,
            deploymentName,
            buildId,
            identity: "flux-real-cluster-test"
          })
          break
        } catch (error) {
          if (i >= 40) throw error
          await new Promise((resolve) => setTimeout(resolve, 250))
        }
      }
      return await client.workflow.execute("deploymentWorkflow", {
        taskQueue,
        workflowId,
        args: [quickCanary("api")]
      }) as DeploymentResult
    })
    expect(result.kind).toBe("Succeeded")

    // The execution must be pinned to the version that ran it.
    const description = await client.workflow.getHandle(workflowId).describe()
    const info = description.raw.workflowExecutionInfo
    expect(info?.workerDeploymentName).toBe(deploymentName)
    expect(info?.versioningInfo?.deploymentVersion?.buildId).toBe(buildId)
  })
}, 240_000)
