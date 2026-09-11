import { describe, it } from "@effect/vitest"
import { Effect, Exit } from "effect"
import { expect } from "vitest"
import { AdmissionController, layer } from "../src/admission.ts"

/**
 * The STM admission controller. The key property is atomicity under
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

  it.effect("seats a whole rollout at once", () =>
    Effect.gen(function*() {
      const admission = yield* AdmissionController
      yield* admission.admitAll(["db", "api", "web"])
      const inFlight = yield* admission.inFlight
      expect([...inFlight].sort()).toEqual(["api", "db", "web"])
    }).pipe(Effect.provide(layer(4))))

  it.effect("seats none of a rollout that does not fit the budget", () =>
    Effect.gen(function*() {
      const admission = yield* AdmissionController
      // Four services, budget of three: the transaction must roll back the
      // seats it had already taken rather than start three quarters of a
      // rollout. This is the property that makes it worth STM instead of a
      // loop over `admit`.
      const rejected = yield* Effect.flip(admission.admitAll(["db", "api", "cache", "web"]))
      expect(rejected._tag).toBe("DeploymentBudgetExhausted")

      const inFlight = yield* admission.inFlight
      expect(inFlight).toHaveLength(0)

      // And the budget is intact: a smaller rollout still fits entirely.
      yield* admission.admitAll(["db", "api", "cache"])
      expect(yield* admission.inFlight).toHaveLength(3)
    }).pipe(Effect.provide(layer(3))))

  it.effect("refuses a rollout containing a service already deploying, seating none", () =>
    Effect.gen(function*() {
      const admission = yield* AdmissionController
      yield* admission.admit("api")

      const rejected = yield* Effect.flip(admission.admitAll(["db", "api", "web"]))
      expect(rejected._tag).toBe("ServiceAlreadyDeploying")

      // "db" was seated before the clash was reached; it must not have stuck.
      const inFlight = yield* admission.inFlight
      expect([...inFlight].sort()).toEqual(["api"])
    }).pipe(Effect.provide(layer(10))))
})
