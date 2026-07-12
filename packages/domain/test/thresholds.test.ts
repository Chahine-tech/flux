import { describe, it } from "@effect/vitest"
import { expect } from "vitest"
import type { Thresholds } from "../src/config.ts"
import type { MetricsSnapshot } from "../src/metrics.ts"
import { evaluateThresholds } from "../src/thresholds.ts"

const thresholds: Thresholds = { maxErrorRate: 0.01, maxP99LatencyMs: 500 }
const snapshot = (errorRate: number, p99LatencyMs: number): MetricsSnapshot => ({
  errorRate,
  p99LatencyMs
})

describe("evaluateThresholds", () => {
  it("is Within when both metrics are at or below their limits", () => {
    expect(evaluateThresholds(snapshot(0.01, 500), thresholds)._tag).toBe("Within")
    expect(evaluateThresholds(snapshot(0, 0), thresholds)._tag).toBe("Within")
  })

  it("breaches on error rate alone", () => {
    const result = evaluateThresholds(snapshot(0.023, 142), thresholds)
    expect(result._tag).toBe("Breached")
    if (result._tag === "Breached") {
      expect(result.breaches).toHaveLength(1)
      expect(result.breaches[0].metric).toBe("errorRate")
      expect(result.breaches[0].observed).toBe(0.023)
      expect(result.breaches[0].limit).toBe(0.01)
    }
  })

  it("breaches on latency alone", () => {
    const result = evaluateThresholds(snapshot(0.005, 900), thresholds)
    expect(result._tag).toBe("Breached")
    if (result._tag === "Breached") {
      expect(result.breaches).toHaveLength(1)
      expect(result.breaches[0].metric).toBe("p99LatencyMs")
    }
  })

  it("reports both breaches when both metrics are over", () => {
    const result = evaluateThresholds(snapshot(0.5, 5000), thresholds)
    expect(result._tag).toBe("Breached")
    if (result._tag === "Breached") {
      expect(result.breaches.map((b) => b.metric)).toEqual(["errorRate", "p99LatencyMs"])
    }
  })

  it("treats exactly-at-limit as within (strictly-greater breaches)", () => {
    expect(evaluateThresholds(snapshot(0.010001, 500), thresholds)._tag).toBe("Breached")
    expect(evaluateThresholds(snapshot(0.01, 500.0001), thresholds)._tag).toBe("Breached")
  })
})
