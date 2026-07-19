import { type ChildProcess, spawn } from "node:child_process"
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { fileURLToPath } from "node:url"
import { afterAll, describe, expect, it } from "vitest"

/**
 * The D23 durability proof: the canary on `ClusterWorkflowEngine` +
 * `SqlMessageStorage` (SQLite) survives a real `SIGKILL` mid-monitor and
 * resumes to completion in a fresh process — the Effect-side counterpart of
 * what D19's real-cluster CI proves for Temporal. Without this, the written
 * comparison would assert durability on proof for one engine and on faith for
 * the other.
 *
 * Hermetic: no Temporal, no docker — two child Node processes and a tmp
 * SQLite file.
 */

const tsxCli = fileURLToPath(import.meta.resolve("tsx/cli"))
const runnerScript = fileURLToPath(new URL("./cluster-runner.ts", import.meta.url))

const children: Array<ChildProcess> = []
const dirs: Array<string> = []

afterAll(() => {
  for (const child of children) {
    if (child.exitCode === null) child.kill("SIGKILL")
  }
  for (const dir of dirs) rmSync(dir, { recursive: true, force: true })
})

const startRunner = (dbPath: string, shiftLog: string) => {
  const child = spawn(process.execPath, [tsxCli, runnerScript, dbPath, shiftLog], {
    stdio: ["ignore", "pipe", "pipe"]
  })
  children.push(child)
  let stdout = ""
  let stderr = ""
  child.stdout.on("data", (chunk: Buffer) => {
    stdout += chunk.toString()
  })
  child.stderr.on("data", (chunk: Buffer) => {
    stderr += chunk.toString()
  })
  return { child, stdout: () => stdout, stderr: () => stderr }
}

const waitFor = async (predicate: () => boolean, timeoutMs: number, what: string): Promise<void> => {
  const deadline = Date.now() + timeoutMs
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`)
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
}

const exited = (child: ChildProcess): Promise<void> =>
  child.exitCode !== null
    ? Promise.resolve()
    : new Promise((resolve) => child.once("exit", () => resolve()))

describe("durability (D23): ClusterWorkflowEngine over SQLite", () => {
  it("survives SIGKILL mid-monitor and resumes to completion in a fresh process", async () => {
    const dir = mkdtempSync(join(tmpdir(), "flux-kill-proof-"))
    dirs.push(dir)
    const dbPath = join(dir, "cluster.db")
    const shiftLog = join(dir, "shifts.log")

    // Phase 1: run until the first traffic shift is on disk (the canary is
    // inside its 3s monitoring window), then kill -9 — no grace, no cleanup.
    const first = startRunner(dbPath, shiftLog)
    await waitFor(
      () => existsSync(shiftLog) && readFileSync(shiftLog, "utf8").includes("v2@10"),
      30_000,
      "first shift (phase 1)"
    )
    await new Promise((resolve) => setTimeout(resolve, 500))
    first.child.kill("SIGKILL")
    await exited(first.child)
    expect(first.stdout()).not.toContain("RESULT:") // it really died mid-flight

    // Phase 2: same SQLite file, same payload → same execution id. The fresh
    // process must resume the persisted execution and finish the rollout.
    const second = startRunner(dbPath, shiftLog)
    await waitFor(() => second.stdout().includes("RESULT:"), 60_000, `resumed result (stderr: ${second.stderr().slice(0, 500)})`)
    const resultLine = second.stdout().split("\n").find((line) => line.startsWith("RESULT:"))!
    const result = JSON.parse(resultLine.slice("RESULT:".length)) as { _tag: string }
    expect(result._tag).toBe("Succeeded")
    await exited(second.child)

    // The sharp assertion: the shift that completed BEFORE the kill was
    // replayed from SQL storage, not re-executed — its side effect must appear
    // exactly once. The 100% shift ran only in phase 2, also exactly once.
    const shifts = readFileSync(shiftLog, "utf8").trim().split("\n")
    expect(shifts.filter((s) => s === "v2@10")).toHaveLength(1)
    expect(shifts.filter((s) => s === "v2@100")).toHaveLength(1)
  }, 120_000)
})
