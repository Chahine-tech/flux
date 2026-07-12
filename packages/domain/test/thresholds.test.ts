import { describe, it } from "@effect/vitest"
import { expect } from "vitest"
import type { Thresholds } from "../src/config.ts"
import { evaluateThresholds, type MetricReadings } from "../src/thresholds.ts"

const rules: Thresholds = [
  { name: "errorRate", query: 'rate(http_errors[1m])', max: 0.01 },
  { name: "p99", query: "histogram_quantile(0.99, ...)", max: 500 }
]
const readings = (errorRate: number, p99: number): MetricReadings => ({ errorRate, p99 })

describe("evaluateThresholds", () => {
  it("is Within when every reading is at or below its rule's max", () => {
    expect(evaluateThresholds(readings(0.01, 500), rules)._tag).toBe("Within")
    expect(evaluateThresholds(readings(0, 0), rules)._tag).toBe("Within")
  })

  it("breaches the rule whose reading is over budget", () => {
    const result = evaluateThresholds(readings(0.023, 142), rules)
    expect(result._tag).toBe("Breached")
    if (result._tag === "Breached") {
      expect(result.breaches).toHaveLength(1)
      expect(result.breaches[0].metric).toBe("errorRate")
      expect(result.breaches[0].observed).toBe(0.023)
      expect(result.breaches[0].limit).toBe(0.01)
    }
  })

  it("reports every breached rule", () => {
    const result = evaluateThresholds(readings(0.5, 5000), rules)
    expect(result._tag).toBe("Breached")
    if (result._tag === "Breached") {
      expect(result.breaches.map((b) => b.metric)).toEqual(["errorRate", "p99"])
    }
  })

  it("treats exactly-at-limit as within (strictly-greater breaches)", () => {
    expect(evaluateThresholds(readings(0.010001, 500), rules)._tag).toBe("Breached")
    expect(evaluateThresholds(readings(0.01, 500.0001), rules)._tag).toBe("Breached")
  })

  it("ignores rules with no reading", () => {
    expect(evaluateThresholds({ errorRate: 0.005 }, rules)._tag).toBe("Within")
  })
})
