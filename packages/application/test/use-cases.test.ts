import { describe, it } from "@effect/vitest"
import { Duration, Effect, Layer } from "effect"
import { HealthCheckFailed } from "@flux/domain"
import { expect } from "vitest"
import { MetricsUnavailable } from "../src/errors.ts"
import { HealthPort } from "../src/ports/health.ts"
import { MetricsPort } from "../src/ports/metrics.ts"
import { healthCheck } from "../src/use-cases/health-check.ts"
import { monitorStep } from "../src/use-cases/monitor-step.ts"

const thresholds = { maxErrorRate: 0.01, maxP99LatencyMs: 500 }

// Test Layers standing in for the real adapters — the ports/adapters payoff.
const metricsReturning = (errorRate: number, p99LatencyMs: number) =>
  Layer.succeed(MetricsPort, { collect: () => Effect.succeed({ errorRate, p99LatencyMs }) })

const metricsFailing = Layer.succeed(MetricsPort, {
  collect: () => Effect.fail(new MetricsUnavailable({ service: "api", reason: "down" }))
})

const healthOk = Layer.succeed(HealthPort, { check: () => Effect.void })
const healthBad = Layer.succeed(HealthPort, {
  check: (p) => Effect.fail(new HealthCheckFailed({ service: p.service, version: p.version, reason: "503" }))
})

describe("monitorStep", () => {
  it.effect("is Within when metrics are under budget", () =>
    Effect.gen(function*() {
      const result = yield* monitorStep({
        service: "api",
        version: "v2",
        window: Duration.minutes(5),
        thresholds
      })
      expect(result._tag).toBe("Within")
    }).pipe(Effect.provide(metricsReturning(0.005, 100))))

  it.effect("is Breached when the error rate is over budget", () =>
    Effect.gen(function*() {
      const result = yield* monitorStep({
        service: "api",
        version: "v2",
        window: Duration.minutes(5),
        thresholds
      })
      expect(result._tag).toBe("Breached")
    }).pipe(Effect.provide(metricsReturning(0.05, 100))))

  it.effect("propagates MetricsUnavailable from the port", () =>
    Effect.gen(function*() {
      const exit = yield* Effect.exit(
        monitorStep({ service: "api", version: "v2", window: Duration.minutes(5), thresholds })
      )
      expect(exit._tag).toBe("Failure")
    }).pipe(Effect.provide(metricsFailing)))
})

describe("healthCheck", () => {
  it.effect("succeeds against a healthy version", () =>
    healthCheck({ service: "api", version: "v2" }).pipe(Effect.provide(healthOk)))

  it.effect("fails with HealthCheckFailed against an unhealthy version", () =>
    Effect.gen(function*() {
      const exit = yield* Effect.exit(healthCheck({ service: "api", version: "v2" }))
      expect(exit._tag).toBe("Failure")
    }).pipe(Effect.provide(healthBad)))
})
