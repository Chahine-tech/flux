import { mkdirSync, writeFileSync } from "node:fs"
import { join } from "node:path"
import { fileURLToPath } from "node:url"
import { Client } from "@temporalio/client"
import { historyToJSON } from "@temporalio/common/lib/proto-utils"
import { TestWorkflowEnvironment } from "@temporalio/testing"
import { bundleWorkflowCode, Worker } from "@temporalio/worker"
import { afterAll, beforeAll, describe, expect, it } from "vitest"
import { type DeploymentInput, type DeploymentResult, type DeploymentState, SEARCH_ATTRIBUTES } from "../src/deployment-input.ts"
import { makePayloadCodec } from "../src/payload-codec.ts"
import type { DeploymentActivities } from "../src/activities/types.ts"

// temporal.api.enums.v1.IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD
const KEYWORD = 2

/**
 * Fixture capture for the replay determinism lock (D22). Not a test of
 * anything: it records the two histories `replay.test.ts` replays, and is
 * gated so it only runs when regenerating them is the deliberate intent:
 *
 *   FLUX_CAPTURE_HISTORIES=1 pnpm --filter @flux/orchestration test -- capture
 *
 * Regenerating is a reviewed act, like updating a snapshot: a workflow change
 * that breaks replay of the committed histories is exactly the change that
 * would break in-flight workflows in production. Capture runs with the D21
 * codec on and an input past the compression threshold, so the fixtures store
 * gzipped payloads — replaying them green is also a codec-symmetry proof.
 */

const TASK_QUEUE = "flux-capture"
const workflowsPath = fileURLToPath(new URL("../src/workflows/index.ts", import.meta.url))
const historiesDir = fileURLToPath(new URL("./histories/", import.meta.url))

const dataConverter = { payloadCodecs: [makePayloadCodec()] }

// The long PromQL pushes the workflow input past the codec threshold (1 KiB).
const baseInput: DeploymentInput = {
  service: "api",
  version: "v2.1.0",
  previousVersion: "v2.0.8",
  steps: [
    { percent: 10, monitorMs: 0, requiresApproval: false },
    { percent: 50, monitorMs: 0, requiresApproval: false },
    { percent: 100, monitorMs: 0, requiresApproval: false }
  ],
  rules: [{ name: "errorRate", query: `sum(rate(errors{pod=~"${"x".repeat(2000)}"}[1m]))`, max: 0.01 }],
  pollIntervalMs: 100
}

const okActivities = (): DeploymentActivities => ({
  healthCheck: async () => {},
  setTrafficWeight: async () => {},
  monitorStep: async () => ({ _tag: "Within" }),
  notify: async () => {},
  readRouterState: async () => [],
  recordOutcome: async () => {}
})

let env: TestWorkflowEnvironment
let client: Client
let workflowBundle: Awaited<ReturnType<typeof bundleWorkflowCode>>

const shouldCapture = process.env.FLUX_CAPTURE_HISTORIES === "1"

beforeAll(async () => {
  if (!shouldCapture) return
  env = await TestWorkflowEnvironment.createTimeSkipping()
  workflowBundle = await bundleWorkflowCode({ workflowsPath })
  // The captured workflow must run exactly as in production: same codec on the
  // client that starts it and on the worker that executes it. Identity is set
  // explicitly — the SDK default is `pid@hostname`, which would commit the
  // capturing machine's name into the fixtures.
  client = new Client({
    connection: env.connection,
    namespace: env.namespace ?? "default",
    dataConverter,
    identity: "flux-capture-client"
  })
  await env.connection.operatorService.addSearchAttributes({
    namespace: env.namespace ?? "default",
    searchAttributes: {
      [SEARCH_ATTRIBUTES.service]: KEYWORD,
      [SEARCH_ATTRIBUTES.version]: KEYWORD,
      [SEARCH_ATTRIBUTES.status]: KEYWORD
    }
  }).catch((error: unknown) => {
    if (!/already exist/i.test(String(error))) throw error
  })
}, 60_000)

afterAll(async () => {
  await env?.teardown()
})

// Proto3 JSON, pretty-printed so a regeneration shows up as a reviewable diff.
const writeFixture = (name: string, historyJson: string) => {
  mkdirSync(historiesDir, { recursive: true })
  writeFileSync(join(historiesDir, `${name}.json`), `${JSON.stringify(JSON.parse(historyJson), null, 2)}\n`)
}

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

describe.skipIf(!shouldCapture)("history fixture capture (D22)", () => {
  it("captures a promotion through the approval gate", async () => {
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
      activities: okActivities(),
      dataConverter,
      identity: "flux-capture-worker"
    })
    await worker.runUntil(async () => {
      const handle = await client.workflow.start("deploymentWorkflow", {
        taskQueue: TASK_QUEUE,
        workflowId: "replay-fixture-promotion",
        args: [input]
      })
      await waitForPhase(() => handle.query<DeploymentState>("status"), "awaiting-approval")
      await handle.executeUpdate("approve")
      const result = (await handle.result()) as DeploymentResult
      expect(result.kind).toBe("Succeeded")
      writeFixture("promotion", historyToJSON(await handle.fetchHistory()))
    })
  })

  it("captures a threshold breach rolling back", async () => {
    let step = 0
    const worker = await Worker.create({
      connection: env.nativeConnection,
      namespace: env.namespace ?? "default",
      taskQueue: TASK_QUEUE,
      workflowBundle,
      activities: {
        ...okActivities(),
        monitorStep: async () =>
          ++step >= 2
            ? { _tag: "Breached" as const, breaches: [{ metric: "errorRate", observed: 0.05, limit: 0.01 }] }
            : { _tag: "Within" as const }
      },
      dataConverter,
      identity: "flux-capture-worker"
    })
    await worker.runUntil(async () => {
      const handle = await client.workflow.start("deploymentWorkflow", {
        taskQueue: TASK_QUEUE,
        workflowId: "replay-fixture-rollback",
        args: [baseInput]
      })
      const result = (await handle.result()) as DeploymentResult
      expect(result.kind).toBe("RolledBack")
      writeFixture("rollback", historyToJSON(await handle.fetchHistory()))
    })
  })
}, 120_000)
