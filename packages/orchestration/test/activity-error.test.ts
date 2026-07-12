import { MetricsUnavailable, NotifyFailed, RouterUnavailable } from "@flux/application"
import { HealthCheckFailed, MetricsThresholdExceeded, RollbackFailed } from "@flux/domain"
import { describe, expect, it } from "vitest"
import { toApplicationFailure } from "../src/activities/activity-error.ts"

describe("toApplicationFailure", () => {
  it("maps business outcomes to non-retryable failures, tagged by _tag", () => {
    const health = toApplicationFailure(
      new HealthCheckFailed({ service: "api", version: "v2", reason: "503" })
    )
    expect(health.nonRetryable).toBe(true)
    expect(health.type).toBe("HealthCheckFailed")
    expect(health.message).toContain("503")

    const thresholds = toApplicationFailure(
      new MetricsThresholdExceeded({ service: "api", atPercent: 50, breaches: [{ metric: "errorRate", observed: 0.05, limit: 0.01 }] })
    )
    expect(thresholds.nonRetryable).toBe(true)
    expect(thresholds.type).toBe("MetricsThresholdExceeded")

    const rollback = toApplicationFailure(
      new RollbackFailed({ service: "api", toVersion: "v1", reason: "nginx down" })
    )
    expect(rollback.nonRetryable).toBe(true)
    expect(rollback.type).toBe("RollbackFailed")
  })

  it("maps infrastructure faults to retryable failures", () => {
    const metrics = toApplicationFailure(new MetricsUnavailable({ service: "api", reason: "timeout" }))
    expect(metrics.nonRetryable).toBe(false)
    expect(metrics.type).toBe("MetricsUnavailable")

    const router = toApplicationFailure(new RouterUnavailable({ service: "api", reason: "reload failed" }))
    expect(router.nonRetryable).toBe(false)
    expect(router.type).toBe("RouterUnavailable")

    const notify = toApplicationFailure(new NotifyFailed({ channel: "slack", reason: "429" }))
    expect(notify.nonRetryable).toBe(false)
    expect(notify.type).toBe("NotifyFailed")
  })

  it("carries the original error as failure details", () => {
    const failure = toApplicationFailure(new HealthCheckFailed({ service: "api", version: "v2", reason: "503" }))
    expect(failure.details?.[0]).toMatchObject({ _tag: "HealthCheckFailed", service: "api" })
  })
})
