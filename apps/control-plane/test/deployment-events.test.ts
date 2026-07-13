import { describe, it } from "@effect/vitest"
import { Effect, Fiber, Layer, Stream } from "effect"
import { TestClock } from "effect/testing"
import type { DeploymentState } from "@flux/contracts"
import { expect } from "vitest"
import { DeploymentEvents, layer } from "../src/deployment-events.ts"
import { TemporalClient } from "../src/temporal-client.ts"

/**
 * Drives the poller under a `TestClock`, with a fake `TemporalClient` whose
 * reported state the test flips between ticks. It pins the two guarantees of
 * D11 in a single sequence: a watcher gets the current state immediately, and
 * the poller publishes a delta only when the state actually changes — an
 * unchanged tick in the middle must not produce a duplicate.
 */

const state = (currentPercent: number, phase: DeploymentState["phase"] = "monitoring"): DeploymentState => ({
  phase,
  service: "api",
  version: "v2",
  currentPercent,
  stepIndex: 0,
  totalSteps: 3
})

// A per-test setup: a closure-held state the test flips, plus the wired layer.
const makeSetup = () => {
  let current = state(10)
  const FakeTemporal = Layer.succeed(TemporalClient, {
    start: () => Effect.succeed("wf1"),
    status: () => Effect.sync(() => current),
    list: () => Effect.succeed([]),
    listRunningIds: () => Effect.succeed(["wf1"]),
    listClosed: () => Effect.succeed([]),
    approve: () => Effect.void,
    abort: () => Effect.void
  })
  const EventsLive = layer({ pollInterval: "5 seconds", maxTracked: 100 }).pipe(Layer.provide(FakeTemporal))
  return { EventsLive, setState: (next: DeploymentState) => (current = next) }
}

describe("deployment events poller", () => {
  it.effect("emits the current state, then only real deltas (unchanged ticks suppressed)", () => {
    const { EventsLive, setState } = makeSetup()
    return Effect.gen(function*() {
      const events = yield* DeploymentEvents
      const collector = yield* events.watch("wf1").pipe(Stream.take(3), Stream.runCollect, Effect.forkChild)

      // Watcher subscribes and emits its immediate current state (10%).
      yield* TestClock.adjust("1 second")
      // Change → next tick publishes 30%.
      setState(state(30))
      yield* TestClock.adjust("5 seconds")
      // No change → the following tick must publish nothing (no duplicate 30%).
      yield* TestClock.adjust("5 seconds")
      // Change → next tick publishes 50%.
      setState(state(50))
      yield* TestClock.adjust("5 seconds")

      const collected = yield* Fiber.join(collector)
      expect(collected.map((s) => s.currentPercent)).toEqual([10, 30, 50])
    }).pipe(Effect.provide(EventsLive))
  })
})
