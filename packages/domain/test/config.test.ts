import { describe, it } from "@effect/vitest"
import { Duration, Schema } from "effect"
import { expect } from "vitest"
import { DeploymentConfig } from "../src/config.ts"

const decode = Schema.decodeUnknownSync(DeploymentConfig)

const valid = {
  service: "api",
  version: "v2.1.0",
  previousVersion: "v2.0.8",
  strategy: {
    _tag: "canary",
    steps: [
      { percent: 10, monitorDuration: "5m", requiresApproval: false },
      { percent: 50, monitorDuration: "10m", requiresApproval: true, approvalTimeout: "1h" },
      { percent: 100, monitorDuration: "0s", requiresApproval: false }
    ]
  },
  thresholds: [
    { name: "errorRate", query: "rate(http_errors[1m])", max: 0.01 },
    { name: "p99", query: "histogram_quantile(0.99, http_latency)", max: 500 }
  ]
}

describe("DeploymentConfig", () => {
  it("decodes a full canary config, turning shorthands into Durations", () => {
    const config = decode(valid)
    expect(config.strategy._tag).toBe("canary")
    const [first, second] = config.strategy.steps
    expect(config.strategy.steps).toHaveLength(3)
    expect(Duration.toMillis(first.monitorDuration)).toBe(300_000)
    expect(second?.approvalTimeout).toBeDefined()
  })

  it("rejects an out-of-range percent", () => {
    const bad = structuredClone(valid)
    bad.strategy.steps[0]!.percent = 150
    expect(() => decode(bad)).toThrow()
  })

  it("rejects a rule with an empty query", () => {
    const bad = structuredClone(valid)
    bad.thresholds[0]!.query = ""
    expect(() => decode(bad)).toThrow()
  })

  it("rejects an empty thresholds list", () => {
    const bad = structuredClone(valid)
    bad.thresholds = []
    expect(() => decode(bad)).toThrow()
  })

  it("rejects an empty service name", () => {
    const bad = structuredClone(valid)
    bad.service = ""
    expect(() => decode(bad)).toThrow()
  })

  it("rejects a strategy with no steps", () => {
    const bad = structuredClone(valid)
    bad.strategy.steps = []
    expect(() => decode(bad)).toThrow()
  })
})
