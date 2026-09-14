import { describe, it } from "@effect/vitest"
import { Effect } from "effect"
import { expect } from "vitest"
import { metricsPrometheusText, recordOutcome, recordTaskQueue, recordTrafficShift } from "../src/metrics.ts"

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

describe("the task-queue gauges", () => {
  it.effect("renders as gauges, labelled by queue, and replace rather than accumulate", () =>
    Effect.gen(function*() {
      yield* recordTaskQueue("flux-deployments", { backlogCount: 5, pollerCount: 2 })

      let text = yield* metricsPrometheusText
      expect(text).toContain("# TYPE flux_task_queue_backlog gauge")
      expect(text).toContain('flux_task_queue_backlog{taskQueue="flux-deployments"} 5')
      expect(text).toContain('flux_task_queue_pollers{taskQueue="flux-deployments"} 2')

      // A gauge is the current depth, not a running total. Reading it again
      // after the queue drains has to say 0, which is the difference from the
      // counters above and the reason `sum()` across workers is wrong for it:
      // every worker reports the same queue, so summing multiplies it.
      yield* recordTaskQueue("flux-deployments", { backlogCount: 0, pollerCount: 3 })
      text = yield* metricsPrometheusText
      expect(text).toContain('flux_task_queue_backlog{taskQueue="flux-deployments"} 0')
      expect(text).toContain('flux_task_queue_pollers{taskQueue="flux-deployments"} 3')
      expect(text).not.toContain('flux_task_queue_backlog{taskQueue="flux-deployments"} 5')
    }))
})
