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
  thresholds: { maxErrorRate: 0.01, maxP99LatencyMs: 500 }
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

  it("rejects an error rate above 1", () => {
    const bad = structuredClone(valid)
    bad.thresholds.maxErrorRate = 2
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
