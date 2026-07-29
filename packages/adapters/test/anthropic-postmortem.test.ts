import { Effect, Layer, Redacted, Result } from "effect"
import { NodeHttpClient } from "@effect/platform-node"
import { postmortem, type RollbackContext } from "@flux/application"
import { afterAll, beforeAll, describe, expect, it } from "vitest"
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http"
import type { AddressInfo } from "node:net"
import * as AnthropicLanguageModel from "../src/ai/anthropic-language-model.ts"
import * as GitHubChangelog from "../src/changelog/github.ts"

/**
 * Hermetic: the postmortem use case, run against the real adapters — the
 * Anthropic provider and the GitHub-compare changelog — both pointed at a local
 * HTTP double. No network, no real key. It proves the wiring the design
 * promises:
 *
 *  1. the changelog source is consulted (`GET /repos/.../compare/v1...v2`) and
 *     its commits are folded into the prompt the model sees — the grounding that
 *     turns the postmortem from a paraphrase of metrics into a correlation with
 *     the actual change;
 *  2. the prompt also carries the breach facts and the auth headers;
 *  3. the completion the model returns reaches the caller as the analysis text.
 *
 * Read what actually crossed the wire, don't take the abstraction's word for it.
 */

const CANNED_ANALYSIS =
  "error_rate hit 0.08 against a 0.01 budget at 50% traffic; the commit swapping the payment SDK is the likely cause."
const COMMIT_SUBJECT = "switch to new payment SDK"

let server: Server
let baseUrl: string
let comparePath: string | undefined
let messagesRequest: { readonly headers: IncomingMessage["headers"]; readonly body: any } | undefined

beforeAll(async () => {
  server = createServer((req: IncomingMessage, res: ServerResponse) => {
    const chunks: Array<Buffer> = []
    req.on("data", (chunk: Buffer) => chunks.push(chunk))
    req.on("end", () => {
      const url = req.url ?? ""
      if (url.includes("/compare/")) {
        // GitHub compare double: one commit between the versions.
        comparePath = url
        res.writeHead(200, { "content-type": "application/json" })
        res.end(JSON.stringify({
          commits: [{ sha: "abcdef1234567", commit: { message: `${COMMIT_SUBJECT}\n\nlonger body ignored` } }]
        }))
        return
      }
      // Anthropic messages double: capture the request, return a canned completion.
      messagesRequest = { headers: req.headers, body: JSON.parse(Buffer.concat(chunks).toString()) }
      res.writeHead(200, { "content-type": "application/json" })
      res.end(JSON.stringify({
        id: "msg_test",
        type: "message",
        role: "assistant",
        content: [{ type: "text", text: CANNED_ANALYSIS }],
        stop_reason: "end_turn"
      }))
    })
  })
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve))
  baseUrl = `http://127.0.0.1:${(server.address() as AddressInfo).port}`
})

afterAll(() => {
  server.close()
})

const context: RollbackContext = {
  service: "checkout",
  version: "v2",
  previousVersion: "v1",
  atPercent: 50,
  breaches: [{ metric: "error_rate", observed: 0.08, limit: 0.01 }]
}

const grounded = () =>
  Layer.mergeAll(
    AnthropicLanguageModel.layer({ apiKey: Redacted.make("test-key"), baseUrl }),
    GitHubChangelog.layer({ repoTemplate: "acme/{service}", token: Redacted.make(""), baseUrl })
  )

describe("grounded rollback postmortem", () => {
  it("consults the changelog and carries both commits and breach facts to the model", async () => {
    const analysis = await Effect.runPromise(
      postmortem(context).pipe(Effect.provide(grounded()), Effect.provide(NodeHttpClient.layerUndici))
    )

    // (3) the completion reaches the caller.
    expect(analysis).toBe(CANNED_ANALYSIS)

    // (1) the changelog was fetched for the right repo and version range.
    expect(comparePath).toBe("/repos/acme/checkout/compare/v1...v2")

    // (2) the request the model saw carried the facts, the commit, and the headers.
    expect(messagesRequest).toBeDefined()
    expect(messagesRequest!.headers["x-api-key"]).toBe("test-key")
    expect(messagesRequest!.headers["anthropic-version"]).toBe("2023-06-01")
    const userTurn = messagesRequest!.body.messages.find((m: { role: string }) => m.role === "user")
    const userText = String(userTurn.content)
    expect(userText).toContain("error_rate")
    expect(userText).toContain("0.08")
    expect(userText).toContain("50%")
    // The grounding: the commit subject (first line only) reached the prompt.
    expect(userText).toContain(COMMIT_SUBJECT)
    expect(userText).not.toContain("longer body ignored")
  })

  it("still produces a postmortem when the changelog source is disabled", async () => {
    const analysis = await Effect.runPromise(
      postmortem(context).pipe(
        Effect.provide(Layer.mergeAll(
          AnthropicLanguageModel.layer({ apiKey: Redacted.make("test-key"), baseUrl }),
          GitHubChangelog.layerNone
        )),
        Effect.provide(NodeHttpClient.layerUndici)
      )
    )
    expect(analysis).toBe(CANNED_ANALYSIS)
    const userTurn = messagesRequest!.body.messages.find((m: { role: string }) => m.role === "user")
    expect(String(userTurn.content)).toContain("(no changelog available)")
  })

  it("fails cleanly when no API key is configured (no network call)", async () => {
    const result = await Effect.runPromise(
      postmortem(context).pipe(
        Effect.provide(Layer.mergeAll(
          AnthropicLanguageModel.layer({ apiKey: Redacted.make(""), baseUrl }),
          GitHubChangelog.layerNone
        )),
        Effect.provide(NodeHttpClient.layerUndici),
        Effect.result
      )
    )
    expect(Result.isFailure(result)).toBe(true)
  })
})
