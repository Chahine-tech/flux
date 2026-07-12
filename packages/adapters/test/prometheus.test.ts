import { describe, it } from "@effect/vitest"
import { Duration } from "effect"
import { expect } from "vitest"
import {
  errorRateQuery,
  extractScalar,
  p99LatencyQuery,
  promRange,
  type QueryResponse
} from "../src/metrics/prometheus.ts"

describe("promRange", () => {
  it("renders durations as Prometheus range selectors in seconds", () => {
    expect(promRange(Duration.minutes(5))).toBe("300s")
    expect(promRange(Duration.seconds(30))).toBe("30s")
  })

  it("floors to at least 1s so range selectors stay valid", () => {
    expect(promRange(Duration.zero)).toBe("1s")
    expect(promRange(Duration.millis(200))).toBe("1s")
  })
})

describe("query builders", () => {
  it("embeds the service label and range", () => {
    const q = errorRateQuery("api", Duration.minutes(5))
    expect(q).toContain('service="api"')
    expect(q).toContain("[300s]")
    expect(q).toContain('status=~"5.."')
  })

  it("converts p99 latency to milliseconds", () => {
    const q = p99LatencyQuery("api", Duration.minutes(10))
    expect(q).toContain("histogram_quantile(0.99")
    expect(q).toContain("[600s]")
    expect(q).toContain("* 1000")
  })
})

describe("extractScalar", () => {
  const withResult = (raw: string): QueryResponse => ({
    status: "success",
    data: { result: [{ value: [1_700_000_000, raw] }] }
  })

  it("parses the stringified value", () => {
    expect(extractScalar(withResult("0.023"))).toBeCloseTo(0.023)
    expect(extractScalar(withResult("142.5"))).toBeCloseTo(142.5)
  })

  it("treats an empty result as 0 (no data)", () => {
    expect(extractScalar({ status: "success", data: { result: [] } })).toBe(0)
  })

  it("collapses non-finite values to 0", () => {
    expect(extractScalar(withResult("NaN"))).toBe(0)
    expect(extractScalar(withResult("not-a-number"))).toBe(0)
  })
})
