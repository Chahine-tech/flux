import { describe, it } from "@effect/vitest"
import { Effect, Exit } from "effect"
import { expect } from "vitest"
import { AdmissionController, layer } from "../src/admission.ts"

/**
 * The STM admission controller (N4/D14). The key property is atomicity under
 * concurrency: many trigger requests racing for the last slots must never
 * over-admit. `Effect.all(..., { concurrency: "unbounded" })` fires the admits
 * in parallel; exactly `budget` of them may win.
 */
describe("admission control", () => {
  it.effect("admits up to the budget and rejects the rest, atomically", () =>
    Effect.gen(function*() {
      const admission = yield* AdmissionController
      const services = ["a", "b", "c", "d", "e"]

      const exits = yield* Effect.all(
        services.map((service) => Effect.exit(admission.admit(service))),
        { concurrency: "unbounded" }
      )

      expect(exits.filter(Exit.isSuccess)).toHaveLength(2)
      const inFlight = yield* admission.inFlight
      expect(inFlight).toHaveLength(2)
    }).pipe(Effect.provide(layer(2))))

  it.effect("releasing a slot lets a new deployment in", () =>
    Effect.gen(function*() {
      const admission = yield* AdmissionController
      yield* admission.admit("a")
      yield* admission.admit("b")

      // Budget of 2 is full → the third is rejected as over-budget.
      const rejected = yield* Effect.flip(admission.admit("c"))
      expect(rejected._tag).toBe("DeploymentBudgetExhausted")

      // Free one and the third gets in.
      yield* admission.release("a")
      yield* admission.admit("c")
      const inFlight = yield* admission.inFlight
      expect([...inFlight].sort()).toEqual(["b", "c"])
    }).pipe(Effect.provide(layer(2))))

  it.effect("rejects a second in-flight deployment of the same service", () =>
    Effect.gen(function*() {
      const admission = yield* AdmissionController
      yield* admission.admit("api")
      const rejected = yield* Effect.flip(admission.admit("api"))
      expect(rejected._tag).toBe("ServiceAlreadyDeploying")
    }).pipe(Effect.provide(layer(10))))
})
