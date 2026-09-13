import { createServer, type Server } from "node:http"
import { Effect } from "effect"
import { HttpClient } from "effect/unstable/http"
import { afterAll, beforeAll, describe, expect, it } from "vitest"
import { BoundedHttpClient } from "../src/http-timeout.ts"

/**
 * The bound that a k3d experiment showed was missing (D50).
 *
 * Blackholing Prometheus with a NetworkPolicy produced no error: the
 * `monitorStep` activity sat in flight for minutes, past its 30 second
 * `heartbeatTimeout`, because that heartbeat is a concurrent daemon proving the
 * process alive rather than the work progressing. So the thing to pin is not
 * "a refused connection fails" (it always did) but "a connection that is
 * accepted and then answered by nobody fails anyway, and quickly".
 *
 * The server here accepts the socket and never writes, which is what a
 * blackholed dependency looks like from the caller's side.
 */
let silent: Server
let port: number

beforeAll(async () => {
  silent = createServer(() => {
    // Deliberately no response, ever.
  })
  await new Promise<void>((resolve) => silent.listen(0, "127.0.0.1", resolve))
  const address = silent.address()
  port = typeof address === "object" && address !== null ? address.port : 0
})

afterAll(() => {
  silent.closeAllConnections()
  silent.close()
})

describe("outbound HTTP from the worker", () => {
  it("fails instead of hanging when the server accepts and never answers", async () => {
    const previous = process.env.FLUX_HTTP_TIMEOUT_SECONDS
    process.env.FLUX_HTTP_TIMEOUT_SECONDS = "1"
    try {
      const started = Date.now()
      const result = await Effect.runPromise(
        Effect.gen(function*() {
          const client = yield* HttpClient.HttpClient
          return yield* client.get(`http://127.0.0.1:${port}/`)
        }).pipe(Effect.provide(BoundedHttpClient), Effect.result)
      )
      const elapsed = Date.now() - started

      expect(result._tag, "the call has to fail, not hang").toBe("Failure")
      if (result._tag !== "Failure") return
      // Bounded by the budget rather than by anything the OS decides: an
      // unbounded call here waits out a TCP timeout measured in minutes.
      expect(elapsed).toBeLessThan(5_000)
      // And it stays an `HttpClientError` carrying a `TransportError`, which is
      // what keeps every adapter's existing mapping (`MetricsUnavailable` and
      // the rest) working unchanged rather than leaking a new error type.
      const failure = result.failure
      expect(failure._tag).toBe("HttpClientError")
      expect(failure.reason._tag).toBe("TransportError")
      expect(String((failure.reason as { readonly cause: unknown }).cause)).toContain("no response within")
    } finally {
      if (previous === undefined) delete process.env.FLUX_HTTP_TIMEOUT_SECONDS
      else process.env.FLUX_HTTP_TIMEOUT_SECONDS = previous
    }
  }, 30_000)
})
