import { Effect, Ref } from "effect"
import { NodeHttpClient } from "@effect/platform-node"
import { MetricsPort } from "@flux/application"
import { afterAll, beforeAll, describe, expect, it } from "vitest"
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http"
import type { AddressInfo } from "node:net"
import {
  layer as httpJsonLayer,
  makeQueryResolver,
  parseQuery,
  query as buildQuery,
  queryVia,
  readAtPath,
  toNumber
} from "../src/metrics/http-json.ts"

describe("query parsing and JSON-path reading", () => {
  it("splits a query into url and path (and rejects a pathless one)", () => {
    expect(parseQuery("https://svc/metrics data.error_rate")).toEqual({
      url: "https://svc/metrics",
      path: "data.error_rate"
    })
    expect(parseQuery("https://svc/metrics")).toBeUndefined()
    expect(buildQuery("https://svc/m", "a.b")).toBe("https://svc/m a.b")
  })

  it("walks dotted paths, indexing arrays by number", () => {
    const json = { data: { checkout: { error_rate: 0.08 } }, series: [{ v: 1 }, { v: 2 }] }
    expect(readAtPath(json, "data.checkout.error_rate")).toBe(0.08)
    expect(readAtPath(json, "series.1.v")).toBe(2)
    expect(readAtPath(json, "data.missing.thing")).toBeUndefined()
  })

  it("coerces numbers and numeric strings, rejects the rest", () => {
    expect(toNumber(0.08)).toBe(0.08)
    expect(toNumber("420")).toBe(420)
    expect(toNumber("nope")).toBeUndefined()
    expect(toNumber(Infinity)).toBeUndefined()
    expect(toNumber(null)).toBeUndefined()
  })
})

describe("RequestResolver deduplication", () => {
  it("fetches each distinct query once, even when rules share it", async () => {
    await Effect.runPromise(
      Effect.gen(function*() {
        const fetches = yield* Ref.make<Array<string>>([])
        const fetch = (q: string) => Ref.update(fetches, (log) => [...log, q]).pipe(Effect.as(q.length))
        const query = queryVia(makeQueryResolver(fetch))

        const results = yield* Effect.forEach(
          ["u p", "u p", "u q"],
          (q) => query(q),
          { concurrency: "unbounded" }
        )
        const log = yield* Ref.get(fetches)
        expect(log.sort()).toEqual(["u p", "u q"]) // deduped: two distinct queries, two fetches
        expect(results).toEqual([3, 3, 3]) // all three requests still resolved
      })
    )
  })
})

// Real-adapter proof against a local HTTP server standing in for a JSON metrics
// endpoint, with a hit counter so the dedup is observed at the transport layer.
let server: Server
let baseUrl: string
let hits = 0

beforeAll(async () => {
  server = createServer((_req: IncomingMessage, res: ServerResponse) => {
    hits += 1
    res.writeHead(200, { "content-type": "application/json" })
    res.end(JSON.stringify({ data: { checkout: { error_rate: 0.08 } }, latency_ms: "420" }))
  })
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve))
  baseUrl = `http://127.0.0.1:${(server.address() as AddressInfo).port}`
})

afterAll(() => {
  server.close()
})

const runQueries = (queries: ReadonlyArray<string>) =>
  Effect.runPromise(
    Effect.gen(function*() {
      const metrics = yield* MetricsPort
      return yield* Effect.forEach(queries, (q) => metrics.query(q), { concurrency: "unbounded" })
    }).pipe(
      Effect.provide(httpJsonLayer()),
      Effect.provide(NodeHttpClient.layerUndici)
    )
  )

describe("HttpJsonMetrics against a real endpoint", () => {
  it("reads a value at the JSON path and dedups a shared query to one request", async () => {
    hits = 0
    const q = `${baseUrl}/metrics data.checkout.error_rate`
    // Two rules share the query; a third reads a different path (coerced string).
    const [a, b, latency] = await runQueries([q, q, `${baseUrl}/metrics latency_ms`])
    expect(a).toBe(0.08)
    expect(b).toBe(0.08)
    expect(latency).toBe(420)
    // The shared query hit the server once; the distinct one hit it once → two.
    expect(hits).toBe(2)
  })

  it("fails when the path is missing (→ MetricsUnavailable → a Failed deployment)", async () => {
    // A successful response whose configured path is absent is unusable data, so
    // the query fails (the same convention the Prometheus adapter's failure test
    // uses through the resolver). That the missing path yields no number is
    // proven directly by the `readAtPath` / `toNumber` unit tests above.
    const exit = await Effect.runPromise(
      Effect.gen(function*() {
        const metrics = yield* MetricsPort
        return yield* Effect.exit(metrics.query(`${baseUrl}/metrics data.missing.thing`))
      }).pipe(
        Effect.provide(httpJsonLayer()),
        Effect.provide(NodeHttpClient.layerUndici)
      )
    )
    expect(exit._tag).toBe("Failure")
  })
})
