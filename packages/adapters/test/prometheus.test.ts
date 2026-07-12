import { describe, it } from "@effect/vitest"
import { Effect, Ref } from "effect"
import { expect } from "vitest"
import { MetricsUnavailable } from "@flux/application"
import {
  errorRateQuery,
  extractScalar,
  makeQueryResolver,
  p99LatencyQuery,
  type QueryResponse,
  queryVia
} from "../src/metrics/prometheus.ts"

describe("query builders", () => {
  it("embed the service label", () => {
    expect(errorRateQuery("api")).toContain('service="api"')
    expect(errorRateQuery("api")).toContain('status=~"5.."')
    expect(p99LatencyQuery("api")).toContain("histogram_quantile(0.99")
    expect(p99LatencyQuery("api")).toContain("* 1000")
  })
})

describe("extractScalar", () => {
  const withResult = (raw: string): QueryResponse => ({
    status: "success",
    data: { result: [{ value: [1_700_000_000, raw] }] }
  })

  it("parses the stringified value", () => {
    expect(extractScalar(withResult("0.023"))).toBeCloseTo(0.023)
  })

  it("treats an empty result as 0 and collapses non-finite values", () => {
    expect(extractScalar({ status: "success", data: { result: [] } })).toBe(0)
    expect(extractScalar(withResult("NaN"))).toBe(0)
  })
})

describe("RequestResolver deduplication", () => {
  it.effect("fetches each distinct query once, even when rules share it", () =>
    Effect.gen(function*() {
      const fetches = yield* Ref.make<Array<string>>([])
      // Track every backend fetch; return a value derived from the query.
      const fetch = (promql: string) =>
        Ref.update(fetches, (log) => [...log, promql]).pipe(Effect.as(promql.length))

      const query = queryVia(makeQueryResolver(fetch))

      // Two rules share "q_errors"; "q_latency" is distinct — 3 requests, 2 fetches.
      const results = yield* Effect.forEach(
        ["q_errors", "q_errors", "q_latency"],
        (q) => query(q),
        { concurrency: "unbounded" }
      )

      const log = yield* Ref.get(fetches)
      expect(log.sort()).toEqual(["q_errors", "q_latency"]) // deduped
      expect(results).toEqual([8, 8, 9]) // all three requests still resolved
    }))

  it.effect("propagates a fetch failure to every batched request", () =>
    Effect.gen(function*() {
      const query = queryVia(
        makeQueryResolver(() => Effect.fail(new MetricsUnavailable({ service: "prometheus", reason: "down" })))
      )
      const exit = yield* Effect.exit(query("q_errors"))
      expect(exit._tag).toBe("Failure")
    }))
})
