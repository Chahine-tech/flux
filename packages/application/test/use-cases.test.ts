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

// A metrics Layer whose sample depends on the poll number, plus a way to read
// how many times it was polled — lets us assert the Stream's polling behaviour.
const countingMetrics = (
  sample: (poll: number) => { readonly errorRate: number; readonly p99LatencyMs: number }
) => {
  let polls = 0
  const layer = Layer.succeed(MetricsPort, {
    collect: () =>
      Effect.sync(() => {
        polls += 1
        return sample(polls)
      })
  })
  return { layer, polls: () => polls }
}

const healthy = { errorRate: 0.002, p99LatencyMs: 100 }
const breaching = { errorRate: 0.05, p99LatencyMs: 900 }

// Tiny durations keep the (real-clock) Stream fast; assertions are on poll
// counts and outcome, which are deterministic regardless of exact timing.
const short = { window: Duration.millis(10), pollInterval: Duration.millis(1) }

describe("monitorStep (Stream)", () => {
  it.effect("polls across the window and reports Within when healthy", () =>
    Effect.gen(function*() {
      const metrics = countingMetrics(() => healthy)
      const result = yield* monitorStep({ service: "api", version: "v2", ...short, thresholds }).pipe(
        Effect.provide(metrics.layer)
      )
      expect(result._tag).toBe("Within")
      // floor(window/interval) + 1 = 11 samples.
      expect(metrics.polls()).toBe(11)
    }))

  it.effect("stops early on the first breach", () =>
    Effect.gen(function*() {
      const metrics = countingMetrics((poll) => (poll >= 3 ? breaching : healthy))
      const result = yield* monitorStep({ service: "api", version: "v2", ...short, thresholds }).pipe(
        Effect.provide(metrics.layer)
      )
      expect(result._tag).toBe("Breached")
      // Stopped at the breaching poll — did not run the full window.
      expect(metrics.polls()).toBe(3)
    }))

  it.effect("propagates MetricsUnavailable from the port", () =>
    Effect.gen(function*() {
      const failing = Layer.succeed(MetricsPort, {
        collect: () => Effect.fail(new MetricsUnavailable({ service: "api", reason: "down" }))
      })
      const exit = yield* Effect.exit(
        monitorStep({ service: "api", version: "v2", ...short, thresholds }).pipe(Effect.provide(failing))
      )
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
