import { type ChildProcess, spawn } from "node:child_process"
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { fileURLToPath } from "node:url"
import { Client, Connection } from "@temporalio/client"
import { NativeConnection, Worker } from "@temporalio/worker"
import type { DeploymentInput, DeploymentResult } from "@flux/orchestration"
import { afterAll, beforeAll, describe, expect, it } from "vitest"
import { ensureSearchAttributes } from "../src/search-attributes.ts"
import { activities } from "./worker-doubles.ts"

/**
 * D27: the worker-SIGKILL resume proof, Temporal side — the symmetric twin of
 * the comparison package's `persistence.test.ts`. The comparison document was
 * asserting Temporal's crash recovery on the server's word; this proves it in
 * this repo: a worker dies mid-monitor (kill -9), the server notices through
 * the missing heartbeats (the workflow sets `heartbeatTimeout: 30s`),
 * reschedules the activity, and a fresh worker completes the canary —
 * replaying the already-completed traffic shift from history instead of
 * re-executing it.
 *
 * Real-cluster gated (D19 pattern): the crash-recovery machinery under proof
 * is the server's, which the time-skipping test server does not exercise.
 */
const REAL = process.env.FLUX_REAL_TEMPORAL === "1"
const address = process.env.TEMPORAL_ADDRESS ?? "localhost:7233"
const namespace = process.env.TEMPORAL_NAMESPACE ?? "default"

const tsxCli = fileURLToPath(import.meta.resolve("tsx/cli"))
const runnerScript = fileURLToPath(new URL("./worker-runner.ts", import.meta.url))
const workflowsPath = fileURLToPath(import.meta.resolve("@flux/orchestration/workflows"))

let client: Client
let nativeConnection: NativeConnection
const children: Array<ChildProcess> = []
const dirs: Array<string> = []

beforeAll(async () => {
  if (!REAL) return
  await ensureSearchAttributes(address, namespace)
  client = new Client({ connection: await Connection.connect({ address }), namespace })
  nativeConnection = await NativeConnection.connect({ address })
}, 120_000)

afterAll(async () => {
  for (const child of children) {
    if (child.exitCode === null) child.kill("SIGKILL")
  }
  for (const dir of dirs) rmSync(dir, { recursive: true, force: true })
  await nativeConnection?.close()
})

const waitFor = async (predicate: () => boolean, timeoutMs: number, what: string): Promise<void> => {
  const deadline = Date.now() + timeoutMs
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`)
    await new Promise((resolve) => setTimeout(resolve, 200))
  }
}

describe.skipIf(!REAL)("worker crash recovery (D27)", () => {
  it("a SIGKILLed worker's canary completes on a fresh worker, without redoing finished activities", async () => {
    const dir = mkdtempSync(join(tmpdir(), "flux-worker-kill-"))
    dirs.push(dir)
    const shiftLog = join(dir, "shifts.log")
    const taskQueue = `flux-worker-kill-${Date.now()}`

    // Worker A: a separate OS process, so the kill is a real kill.
    const workerA = spawn(process.execPath, [tsxCli, runnerScript, taskQueue, shiftLog], {
      stdio: ["ignore", "pipe", "pipe"],
      env: { ...process.env, TEMPORAL_ADDRESS: address, TEMPORAL_NAMESPACE: namespace }
    })
    children.push(workerA)
    let workerAOut = ""
    workerA.stdout.on("data", (chunk: Buffer) => {
      workerAOut += chunk.toString()
    })
    await waitFor(() => workerAOut.includes("WORKER_READY"), 60_000, "worker A boot")

    const input: DeploymentInput = {
      service: "crash",
      version: "v2",
      previousVersion: "v1",
      // The monitor runs ~8s in the activity double — wide enough that the
      // kill below reliably lands inside it.
      steps: [
        { percent: 10, monitorMs: 8_000, requiresApproval: false },
        { percent: 100, monitorMs: 0, requiresApproval: false }
      ],
      rules: [{ name: "errorRate", query: "q", max: 0.01 }],
      pollIntervalMs: 500
    }
    const workflowId = `worker-kill-${Date.now()}`
    const handle = await client.workflow.start("deploymentWorkflow", {
      taskQueue,
      workflowId,
      args: [input]
    })

    // Kill -9 once the first shift is done and the monitor is in flight.
    await waitFor(
      () => existsSync(shiftLog) && readFileSync(shiftLog, "utf8").includes("monitor-start"),
      60_000,
      "monitor in flight on worker A"
    )
    await new Promise((resolve) => setTimeout(resolve, 1000))
    workerA.kill("SIGKILL")

    // Worker B: in-process, same task queue, same logging doubles. The server
    // must notice A's death (missed heartbeats) and reschedule onto B.
    const workerB = await Worker.create({
      connection: nativeConnection,
      namespace,
      taskQueue,
      workflowsPath,
      activities: activities(shiftLog)
    })
    const result = (await workerB.runUntil(handle.result())) as DeploymentResult
    expect(result.kind).toBe("Succeeded")

    const lines = readFileSync(shiftLog, "utf8").trim().split("\n")
    // The shift that completed on worker A replayed from history on worker B —
    // not re-executed: exactly one v2@10 (and one v2@100, from B only).
    expect(lines.filter((l) => l === "v2@10")).toHaveLength(1)
    expect(lines.filter((l) => l === "v2@100")).toHaveLength(1)
    // The in-flight monitor is the one thing that DID re-run: once on A
    // (killed mid-way), once retried on B.
    expect(lines.filter((l) => l === "monitor-start")).toHaveLength(2)
  }, 180_000)
}, 240_000)
