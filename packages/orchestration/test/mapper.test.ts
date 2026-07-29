import { describe, it } from "@effect/vitest"
import { Schema } from "effect"
import { DeploymentConfig } from "@flux/domain"
import { expect } from "vitest"
import { configToInput } from "../src/mapper.ts"

const config = Schema.decodeUnknownSync(DeploymentConfig)({
  service: "api",
  version: "v2.1.0",
  previousVersion: "v2.0.8",
  strategy: {
    _tag: "canary",
    steps: [
      { percent: 10, monitorDuration: "5m", requiresApproval: false },
      { percent: 50, monitorDuration: "10m", requiresApproval: true, approvalTimeout: "1h" }
    ]
  },
  thresholds: [
    { name: "errorRate", query: "rate(http_errors[1m])", max: 0.01 },
    { name: "p99", query: "histogram_quantile(0.99, http_latency)", max: 500 }
  ]
})

/** Narrow to the canary steps, failing the test if the strategy isn't canary. */
const canarySteps = (input: ReturnType<typeof configToInput>) => {
  if (input.strategy?.kind !== "canary") throw new Error("expected a canary strategy")
  return input.strategy.steps
}

describe("configToInput", () => {
  it("converts Effect Durations to milliseconds", () => {
    const steps = canarySteps(configToInput(config))
    expect(steps[0]?.monitorMs).toBe(300_000)
    expect(steps[1]?.monitorMs).toBe(600_000)
    expect(steps[1]?.approvalTimeoutMs).toBe(3_600_000)
  })

  it("omits approvalTimeoutMs when absent", () => {
    expect(canarySteps(configToInput(config))[0]?.approvalTimeoutMs).toBeUndefined()
  })

  it("maps a blue-green strategy, converting the bake shorthand (D32)", () => {
    const bgConfig = Schema.decodeUnknownSync(DeploymentConfig)({
      service: "api",
      version: "v2.1.0",
      previousVersion: "v2.0.8",
      strategy: { _tag: "blue-green", bakeDuration: "10m", requiresApproval: true, approvalTimeout: "1h" },
      thresholds: [{ name: "errorRate", query: "rate(http_errors[1m])", max: 0.01 }]
    })
    const input = configToInput(bgConfig)
    expect(input.strategy?.kind).toBe("blue-green")
    if (input.strategy?.kind !== "blue-green") throw new Error("expected blue-green")
    expect(input.strategy.bakeMs).toBe(600_000)
    expect(input.strategy.requiresApproval).toBe(true)
    expect(input.strategy.approvalTimeoutMs).toBe(3_600_000)
  })

  it("carries service, versions and metric rules through unchanged", () => {
    const input = configToInput(config)
    expect(input.service).toBe("api")
    expect(input.version).toBe("v2.1.0")
    expect(input.previousVersion).toBe("v2.0.8")
    expect(input.rules).toEqual([
      { name: "errorRate", query: "rate(http_errors[1m])", max: 0.01 },
      { name: "p99", query: "histogram_quantile(0.99, http_latency)", max: 500 }
    ])
  })
})
