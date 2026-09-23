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

describe("monitorStep with verdicts pushed in", () => {
  // No metrics are consulted at all here: the rule is judged on what was
  // reported, so the port is present only because the use case requires it.
  const noMetrics = Layer.succeed(MetricsPort, { query: () => Effect.succeed(0) })
  const outcomeRule = { name: "taskFailureRate", max: 0.05 }

  const judge = (outcomes: { total: number; failures: number } | undefined) =>
    monitorStep({
      service: "agent",
      version: "v2",
      window: Duration.millis(0),
      pollInterval: Duration.millis(1),
      rules: [],
      outcomeRule,
      outcomes
    }).pipe(Effect.provide(noMetrics))

  it.live("treats no verdicts as undecided, not as healthy", () =>
    Effect.gen(function*() {
      // The failing case a bare threshold gets wrong: zero out of zero is a 0%
      // failure rate, and 0% is under any limit. Nothing observed is not the
      // same as nothing wrong.
      expect((yield* judge(undefined))._tag).toBe("Inconclusive")
      expect((yield* judge({ total: 0, failures: 0 }))._tag).toBe("Inconclusive")
    }))

  it.live("is still undecided on a handful of verdicts", () =>
    Effect.gen(function*() {
      // 1 failure in 30 is 3.3% against a 5% limit. The bare rule promotes; the
      // interval reaches 16.7%, so this says it does not know yet.
      expect((yield* judge({ total: 30, failures: 1 }))._tag).toBe("Inconclusive")
    }))

  it.live("clears the rule once enough verdicts agree", () =>
    Effect.gen(function*() {
      expect((yield* judge({ total: 200, failures: 0 }))._tag).toBe("Within")
    }))

  it.live("breaches when the failures are real", () =>
    Effect.gen(function*() {
      const result = yield* judge({ total: 500, failures: 100 })
      expect(result._tag).toBe("Breached")
      if (result._tag === "Breached") {
        expect(result.breaches[0].metric).toBe("taskFailureRate")
        expect(result.breaches[0].observed).toBeCloseTo(0.2, 3)
      }
    }))

  it.live("leaves the verdict rule out when none was configured", () =>
    Effect.gen(function*() {
      const result = yield* monitorStep({
        service: "agent",
        version: "v2",
        window: Duration.millis(0),
        pollInterval: Duration.millis(1),
        rules: [],
        outcomes: { total: 3, failures: 3 }
      }).pipe(Effect.provide(noMetrics))
      // Verdicts with no rule to judge them are not a silent breach.
      expect(result._tag).toBe("Within")
    }))
})
