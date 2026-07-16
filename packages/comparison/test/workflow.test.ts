import { Duration, Effect, Exit, Fiber, Layer } from "effect"
import { TestClock } from "effect/testing"
import type { SetTrafficWeightParams } from "@flux/application"
import { HealthPort, MetricsPort, NotifyPort, RouterPort } from "@flux/application"
import type { CanaryStep, MetricRule } from "@flux/domain"
import { HealthCheckFailed } from "@flux/domain"
import { describe, expect, it } from "@effect/vitest"
import { DurableDeferred } from "effect/unstable/workflow"
import { layerMemory } from "effect/unstable/workflow/WorkflowEngine"
import { ApprovalGate, DeploymentWorkflow, DeploymentWorkflowLive } from "../src/workflow.ts"

/**
 * Mirrors `deployment.workflow.test.ts` scenario-for-scenario, on
 * `effect/unstable/workflow`'s in-memory engine (no durability — that is a
 * separate, deliberately smaller proof; see ARCHITECTURE.md D23). Same test
 * doubles pattern as `packages/application/test/use-cases.test.ts`.
 */

const okHealth = Layer.succeed(HealthPort, { check: () => Effect.void })
const okNotify = Layer.succeed(NotifyPort, { send: () => Effect.void })

const recordingRouter = () => {
  const shifts: Array<SetTrafficWeightParams> = []
  const layer = Layer.succeed(RouterPort, {
    setTrafficWeight: (params) =>
      Effect.sync(() => {
        shifts.push(params)
      }),
    readState: () => Effect.succeed([])
  })
  return { layer, shifts }
}

const metricsAt = (breachAfterPolls: number) => {
  let polls = 0
  return Layer.succeed(MetricsPort, {
    query: () =>
      Effect.sync(() => {
        polls += 1
        return polls >= breachAfterPolls ? 1 : 0
      })
  })
}

const threeSteps: readonly [CanaryStep, ...Array<CanaryStep>] = [
  { percent: 10, monitorDuration: Duration.millis(10), requiresApproval: false },
  { percent: 50, monitorDuration: Duration.millis(10), requiresApproval: false },
  { percent: 100, monitorDuration: Duration.millis(10), requiresApproval: false }
]

const oneRule: readonly [MetricRule, ...Array<MetricRule>] = [
  { name: "errorRate", query: "q", max: 0.5 }
]

const basePayload = {
  service: "api",
  version: "v2",
  previousVersion: "v1",
  steps: threeSteps,
  rules: oneRule
}

const testLayer = (router: ReturnType<typeof recordingRouter>["layer"], metrics: Layer.Layer<MetricsPort>) =>
  DeploymentWorkflowLive.pipe(
    Layer.provide([okHealth, okNotify, router, metrics]),
    Layer.provideMerge(layerMemory)
  )

/**
 * `monitorStepActivity` runs through two nested `Effect.forkChild` boundaries
 * (the workflow engine's own activity wrapper, on top of `monitorStep`'s
 * Stream/Schedule) — a single large `TestClock.adjust` does not reliably
 * cascade through both under the in-memory engine the way it does for a bare
 * `Stream.schedule` call (as in `use-cases.test.ts`). Pumping in small
 * increments with a yield between each is the reliable shape; found
 * empirically, noted for the comparison (D23).
 */
const pumpClock = Effect.gen(function*() {
  for (let i = 0; i < 100; i++) {
    yield* TestClock.adjust(Duration.millis(5))
    yield* Effect.yieldNow
  }
})

describe("DeploymentWorkflow (effect/unstable/workflow, D23)", () => {
  it.effect("succeeds when every step stays within budget", () =>
    Effect.gen(function*() {
      const router = recordingRouter()
      const fiber = yield* DeploymentWorkflow.execute(basePayload).pipe(
        Effect.provide(testLayer(router.layer, metricsAt(Infinity))),
        Effect.forkChild({ startImmediately: true })
      )
      yield* pumpClock
      const result = yield* Fiber.join(fiber)
      expect(result._tag).toBe("Succeeded")
      expect(router.shifts.map((s) => s.weight)).toEqual([10, 50, 100])
    }))

  it.effect("rolls back and restores the previous version when a step breaches (saga via typed error)", () =>
    Effect.gen(function*() {
      const router = recordingRouter()
      const fiber = yield* DeploymentWorkflow.execute(basePayload).pipe(
        Effect.provide(testLayer(router.layer, metricsAt(2))),
        Effect.forkChild({ startImmediately: true })
      )
      yield* pumpClock
      const failure = yield* Fiber.join(fiber).pipe(Effect.flip)
      expect(failure._tag).toBe("RolledBack")
      if (failure._tag === "RolledBack") {
        expect(failure.atPercent).toBe(10)
        expect(failure.toVersion).toBe("v1")
      }
      // withCompensation ran: the last recorded shift restores v1 to 100%.
      expect(router.shifts.at(-1)).toEqual({ service: "api", version: "v1", weight: 100 })
    }))

  it.effect("parks at an approval gate and advances once it is resolved", () =>
    Effect.gen(function*() {
      const router = recordingRouter()
      const payload = {
        ...basePayload,
        steps: [{ percent: 50, monitorDuration: Duration.millis(10), requiresApproval: true }] as readonly [
          CanaryStep,
          ...Array<CanaryStep>
        ]
      }

      // One `Effect.provide` for the whole sequence: `layerMemory`'s engine is
      // fresh state built inside the layer, so providing it separately to
      // `execute`, `tokenFromPayload`, and `done` — as three top-level
      // `Effect.provide` calls — would build three unconnected instances, and
      // `done` would resolve a deferred no running execution can ever see.
      yield* Effect.gen(function*() {
        const fiber = yield* DeploymentWorkflow.execute(payload).pipe(Effect.forkChild({ startImmediately: true }))
        yield* pumpClock

        // Not resolved yet: the fiber is parked awaiting the gate, only one shift happened.
        expect(router.shifts).toHaveLength(1)

        const token = yield* DurableDeferred.tokenFromPayload(ApprovalGate, { workflow: DeploymentWorkflow, payload })
        yield* DurableDeferred.done(ApprovalGate, { token, exit: Exit.void })
        // Resuming re-executes the handler from the top (replay, like
        // Temporal) — earlier activities replay from cache, but the resumed
        // run still needs the clock pumped again for anything not cached yet.
        yield* pumpClock

        const result = yield* Fiber.join(fiber)
        expect(result._tag).toBe("Succeeded")
      }).pipe(Effect.provide(testLayer(router.layer, metricsAt(Infinity))))
    }))

  it.effect("turns a health-check failure into a typed workflow error, no traffic shifted", () =>
    Effect.gen(function*() {
      const router = recordingRouter()
      const failingHealth = Layer.succeed(HealthPort, {
        check: () => Effect.fail(new HealthCheckFailed({ service: "api", version: "v2", reason: "probe 503" }))
      })
      const layer = DeploymentWorkflowLive.pipe(
        Layer.provide([failingHealth, okNotify, router.layer, metricsAt(Infinity)]),
        Layer.provideMerge(layerMemory)
      )
      const failure = yield* DeploymentWorkflow.execute(basePayload).pipe(Effect.provide(layer), Effect.flip)
      expect(failure._tag).toBe("HealthCheckFailed")
      expect(router.shifts).toHaveLength(0)
    }))
})
