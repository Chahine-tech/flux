import { describe, it } from "@effect/vitest"
import { Duration, Effect, Fiber, Layer } from "effect"
import { TestClock } from "effect/testing"
import { HealthCheckFailed, type Thresholds } from "@flux/domain"
import { expect } from "vitest"
import { MetricsUnavailable } from "../src/errors.ts"
import { HealthPort } from "../src/ports/health.ts"
import { MetricsPort } from "../src/ports/metrics.ts"
import { healthCheck } from "../src/use-cases/health-check.ts"
import { monitorStep } from "../src/use-cases/monitor-step.ts"

const rules: Thresholds = [
  { name: "errorRate", query: "q_errors", max: 0.01 },
  { name: "p99", query: "q_latency", max: 500 }
]

// A metrics Layer whose value depends on the poll number, plus a poll counter.
const countingMetrics = (sample: (poll: number) => number) => {
  let polls = 0
  const layer = Layer.succeed(MetricsPort, {
    query: () =>
      Effect.sync(() => {
        polls += 1
        return sample(polls)
      })
  })
  return { layer, polls: () => polls }
}

const forkMonitor = (metricsLayer: Layer.Layer<MetricsPort>) =>
  monitorStep({
    service: "api",
    version: "v2",
    window: Duration.millis(10),
    pollInterval: Duration.millis(1),
    rules
  }).pipe(Effect.provide(metricsLayer), Effect.forkChild)

describe("monitorStep (Stream)", () => {
  it.effect("polls across the whole window and reports Within when healthy", () =>
    Effect.gen(function*() {
      const metrics = countingMetrics(() => 0)
      const fiber = yield* forkMonitor(metrics.layer)
      yield* TestClock.adjust(Duration.millis(20))
      const result = yield* Fiber.join(fiber)
      expect(result._tag).toBe("Within")
      // 11 polls x 2 rules = 22 query calls.
      expect(metrics.polls()).toBe(22)
    }))

  it.effect("stops early on the first breach", () =>
    Effect.gen(function*() {
      // Poll 1 = 2 calls (healthy). Poll 2's first call breaches errorRate.
      const metrics = countingMetrics((poll) => (poll >= 3 ? 0.05 : 0))
      const fiber = yield* forkMonitor(metrics.layer)
      yield* TestClock.adjust(Duration.millis(20))
      const result = yield* Fiber.join(fiber)
      expect(result._tag).toBe("Breached")
    }))

  it.effect("propagates MetricsUnavailable from the port", () =>
    Effect.gen(function*() {
      const failing = Layer.succeed(MetricsPort, {
        query: () => Effect.fail(new MetricsUnavailable({ service: "api", reason: "down" }))
      })
      const fiber = yield* forkMonitor(failing)
      yield* TestClock.adjust(Duration.millis(20))
      const exit = yield* Effect.exit(Fiber.join(fiber))
      expect(exit._tag).toBe("Failure")
    }))
})

describe("healthCheck", () => {
  const healthOk = Layer.succeed(HealthPort, { check: () => Effect.void })
  const healthBad = Layer.succeed(HealthPort, {
    check: (p) => Effect.fail(new HealthCheckFailed({ service: p.service, version: p.version, reason: "503" }))
  })

  it.effect("succeeds against a healthy version", () =>
    healthCheck({ service: "api", version: "v2" }).pipe(Effect.provide(healthOk)))

  it.effect("fails with HealthCheckFailed against an unhealthy version", () =>
    Effect.gen(function*() {
      const exit = yield* Effect.exit(healthCheck({ service: "api", version: "v2" }))
      expect(exit._tag).toBe("Failure")
    }).pipe(Effect.provide(healthBad)))
})
