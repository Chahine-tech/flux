import { describe, it } from "@effect/vitest"
import { Effect } from "effect"
import { expect } from "vitest"
import { metricsPrometheusText, recordOutcome, recordTrafficShift } from "../src/metrics.ts"

describe("metrics Prometheus exposition", () => {
  it.effect("renders counters with HELP/TYPE and labelled samples", () =>
    Effect.gen(function*() {
      yield* recordOutcome("Succeeded")
      yield* recordOutcome("RolledBack")
      yield* recordOutcome("RolledBack")
      yield* recordTrafficShift
      yield* recordTrafficShift

      const text = yield* metricsPrometheusText

      expect(text).toContain("# TYPE flux_deployments_total counter")
      expect(text).toContain('flux_deployments_total{outcome="Succeeded"} 1')
      expect(text).toContain('flux_deployments_total{outcome="RolledBack"} 2')
      expect(text).toContain("# TYPE flux_traffic_shifts_total counter")
      expect(text).toContain("flux_traffic_shifts_total 2")
      // HELP appears once per metric family.
      expect(text.match(/# HELP flux_deployments_total/g)?.length).toBe(1)
    }))
})
