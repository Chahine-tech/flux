import { readdirSync, readFileSync } from "node:fs"
import { join } from "node:path"
import { fileURLToPath } from "node:url"
import { historyFromJSON } from "@temporalio/common/lib/proto-utils"
import { bundleWorkflowCode, Worker } from "@temporalio/worker"
import { beforeAll, describe, expect, it } from "vitest"
import { makePayloadCodec } from "../src/payload-codec.ts"

/**
 * The determinism lock (D22): every history committed under `histories/` is
 * replayed against the current workflow code. A change that diverges from what
 * a past run recorded — the change that would break in-flight workflows in
 * production — fails here as a `DeterminismViolationError`, in the hermetic CI
 * job, with no cluster. Fixtures are regenerated deliberately (see
 * `capture-histories.test.ts`), never by CI.
 */

const workflowsPath = fileURLToPath(new URL("../src/workflows/index.ts", import.meta.url))
const historiesDir = fileURLToPath(new URL("./histories/", import.meta.url))

// The fixtures were captured with the D21 codec on, so replay needs it too:
// a gzipped history replaying green is also proof the codec is symmetric.
const dataConverter = { payloadCodecs: [makePayloadCodec()] }

const fixtures = readdirSync(historiesDir).filter((file) => file.endsWith(".json"))

let workflowBundle: Awaited<ReturnType<typeof bundleWorkflowCode>>

beforeAll(async () => {
  workflowBundle = await bundleWorkflowCode({ workflowsPath })
}, 60_000)

describe("replay determinism lock (D22)", () => {
  it("has the two committed scenarios", () => {
    expect(fixtures).toContain("promotion.json")
    expect(fixtures).toContain("rollback.json")
  })

  it("stores the workflow input gzipped — the codec is part of what replay proves", () => {
    const promotion = JSON.parse(readFileSync(join(historiesDir, "promotion.json"), "utf8"))
    const input = promotion.events[0].workflowExecutionStartedEventAttributes.input.payloads[0]
    expect(Buffer.from(input.metadata.encoding, "base64").toString()).toBe("binary/gzip")
  })

  it("replays every committed history without a determinism violation", async () => {
    const histories = fixtures.map((file) => ({
      workflowId: file.replace(/\.json$/, ""),
      history: historyFromJSON(JSON.parse(readFileSync(join(historiesDir, file), "utf8")))
    }))

    const results = []
    for await (const result of Worker.runReplayHistories({ workflowBundle, dataConverter }, histories)) {
      results.push(result)
    }

    expect(results).toHaveLength(histories.length)
    for (const result of results) {
      expect(result.error, `replay of ${result.workflowId} diverged: ${result.error}`).toBeUndefined()
    }
  })
}, 120_000)
