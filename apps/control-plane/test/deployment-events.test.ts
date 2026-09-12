import { describe, it } from "@effect/vitest"
import { Effect, Fiber, Layer, Stream } from "effect"
import { TestClock } from "effect/testing"
import type { DeploymentState } from "@flux/contracts"
import { expect } from "vitest"
import { DeploymentEvents, keepMonotonic, layer } from "../src/deployment-events.ts"
import { TemporalClient } from "../src/temporal-client.ts"

/**
 * Drives the poller under a `TestClock`, with a fake `TemporalClient` whose
 * reported state the test flips between ticks. It pins the two guarantees of
 * In a single sequence: a watcher gets the current state immediately, and
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
    startMulti: () => Effect.succeed("multi"),
    status: () => Effect.sync(() => current),
    list: () => Effect.succeed([]),
    listRunningIds: () => Effect.succeed(["wf1"]),
    listClosed: () => Effect.succeed([]),
    approve: () => Effect.void,
    abort: () => Effect.void,
    ensureDriftSchedule: () => Effect.succeed("flux-drift-api"),
    disableDrift: () => Effect.void,
    reachable: Effect.succeed(true)
  })
  const EventsLive = layer({ pollInterval: "5 seconds", maxTracked: 100 }).pipe(Layer.provide(FakeTemporal))
  return { EventsLive, setState: (next: DeploymentState) => (current = next) }
}

describe("keepMonotonic", () => {
  it("lets a forward sequence through untouched", () => {
    const keep = keepMonotonic()
    expect([{ readAt: 1 }, { readAt: 2 }, { readAt: 3 }].filter(keep)).toHaveLength(3)
  })

  it("drops a state read before one already emitted", () => {
    const keep = keepMonotonic()
    // The shape of the bug: a subscriber's snapshot is read at 10, then a
    // poller tick that had read the same workflow at 5 publishes afterwards.
    const kept = [{ readAt: 10 }, { readAt: 5 }, { readAt: 12 }].filter(keep)
    expect(kept.map((entry) => entry.readAt)).toEqual([10, 12])
  })

  it("keeps a repeat of the same instant, leaving deduplication to sameState", () => {
    const keep = keepMonotonic()
    expect([{ readAt: 7 }, { readAt: 7 }].filter(keep)).toHaveLength(2)
  })

  it("gives each subscription its own cursor", () => {
    const first = keepMonotonic()
    const second = keepMonotonic()
    expect(first({ readAt: 100 })).toBe(true)
    // A fresh subscriber is not held back by another one's position.
    expect(second({ readAt: 3 })).toBe(true)
    expect(first({ readAt: 3 })).toBe(false)
  })
})

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

  it.effect("does not repeat the state the snapshot already showed", () => {
    // The real sequence behind a duplicated line in `flux status --watch`:
    // `flux deploy` starts a workflow, the watcher connects a moment later and
    // is shown the current state, and only *then* does the poller see the
    // workflow for the first time. A first sighting is a delta to the poller
    // (`lastSeen` has no prior), so it publishes a state this subscriber has
    // already been given. Subscribing before reading the snapshot is what makes
    // it reachable, and losing an event would be the worse trade — so the
    // repeat is collapsed in `watch` instead.
    let current = state(10)
    let running: Array<string> = []
    const FakeTemporal = Layer.succeed(TemporalClient, {
      start: () => Effect.succeed("wf1"),
      startMulti: () => Effect.succeed("multi"),
      status: () => Effect.sync(() => current),
      list: () => Effect.succeed([]),
      listRunningIds: () => Effect.sync(() => running),
      listClosed: () => Effect.succeed([]),
      approve: () => Effect.void,
      abort: () => Effect.void,
      ensureDriftSchedule: () => Effect.succeed("flux-drift-api"),
      disableDrift: () => Effect.void,
    reachable: Effect.succeed(true)
    })
    const EventsLive = layer({ pollInterval: "5 seconds", maxTracked: 100 }).pipe(Layer.provide(FakeTemporal))

    return Effect.gen(function*() {
      const events = yield* DeploymentEvents
      const collector = yield* events.watch("wf1").pipe(Stream.take(2), Stream.runCollect, Effect.forkChild)

      // The watcher subscribes and is shown the current state (10%), while the
      // poller has not yet seen this workflow at all.
      yield* TestClock.adjust("1 second")
      // It shows up in the running set: the next tick is its first sighting.
      running = ["wf1"]
      yield* TestClock.adjust("5 seconds")
      // A real change, which must still come through.
      current = state(30)
      yield* TestClock.adjust("5 seconds")

      const collected = yield* Fiber.join(collector)
      expect(collected.map((s) => s.currentPercent)).toEqual([10, 30])
    }).pipe(Effect.provide(EventsLive))
  })

  it.effect("calls onDeploymentEnded when a tracked deployment leaves the running set", () => {
    let running = ["wf1"]
    const ended: Array<string> = []
    const FakeTemporal = Layer.succeed(TemporalClient, {
      start: () => Effect.succeed("wf1"),
      startMulti: () => Effect.succeed("multi"),
      status: () => Effect.succeed(state(50)), // service "api"
      list: () => Effect.succeed([]),
      listRunningIds: () => Effect.sync(() => running),
      listClosed: () => Effect.succeed([]),
      approve: () => Effect.void,
      abort: () => Effect.void,
      ensureDriftSchedule: () => Effect.succeed("flux-drift-api"),
      disableDrift: () => Effect.void,
    reachable: Effect.succeed(true)
    })
    const EventsLive = layer({
      pollInterval: "5 seconds",
      maxTracked: 100,
      onDeploymentEnded: (service) => Effect.sync(() => ended.push(service))
    }).pipe(Layer.provide(FakeTemporal))

    return Effect.gen(function*() {
      yield* DeploymentEvents
      yield* TestClock.adjust("1 second") // first tick tracks wf1 (running)
      running = [] // the deployment finishes
      yield* TestClock.adjust("5 seconds") // next tick sees it gone → released
      expect(ended).toEqual(["api"])
    }).pipe(Effect.provide(EventsLive))
  })

  it.effect("re-seats a deployment the first time a tick sees it running", () => {
    // What a control-plane restart looks like from the poller's side: the STM
    // admission map is empty, but deployments are already running. Without this
    // the budget stays too permissive until they finish.
    const seen: Array<string> = []
    let running = ["wf1"]
    const FakeTemporal = Layer.succeed(TemporalClient, {
      start: () => Effect.succeed("wf1"),
      startMulti: () => Effect.succeed("multi"),
      status: () => Effect.succeed(state(10)),
      list: () => Effect.succeed([]),
      listRunningIds: () => Effect.sync(() => running),
      listClosed: () => Effect.succeed([]),
      approve: () => Effect.void,
      abort: () => Effect.void,
      ensureDriftSchedule: () => Effect.succeed("flux-drift-api"),
      disableDrift: () => Effect.void,
    reachable: Effect.succeed(true)
    })
    const EventsLive = layer({
      pollInterval: "5 seconds",
      maxTracked: 100,
      onDeploymentSeen: (service) => Effect.sync(() => void seen.push(service))
    }).pipe(Layer.provide(FakeTemporal))

    return Effect.gen(function*() {
      yield* DeploymentEvents
      // The poll loop is forked, so let its first tick run.
      yield* TestClock.adjust("1 second")
      expect(seen).toEqual(["api"])

      // Later ticks must not re-seat it: admission is idempotent, but firing
      // every 5s would bury the real signal.
      yield* TestClock.adjust("5 seconds")
      yield* TestClock.adjust("5 seconds")
      expect(seen).toEqual(["api"])

      // It ends, then a new deployment of the same service starts: seen again.
      running = []
      yield* TestClock.adjust("5 seconds")
      running = ["wf1"]
      yield* TestClock.adjust("5 seconds")
      expect(seen).toEqual(["api", "api"])
    }).pipe(Effect.provide(EventsLive))
  })

  it.effect("closes the watch stream once the deployment reaches a terminal outcome", () => {
    let running = ["wf1"]
    let current: DeploymentState = state(50)
    const FakeTemporal = Layer.succeed(TemporalClient, {
      start: () => Effect.succeed("wf1"),
      startMulti: () => Effect.succeed("multi"),
      status: () => Effect.sync(() => current),
      list: () => Effect.succeed([]),
      listRunningIds: () => Effect.sync(() => running),
      listClosed: () => Effect.succeed([]),
      approve: () => Effect.void,
      abort: () => Effect.void,
      ensureDriftSchedule: () => Effect.succeed("flux-drift-api"),
      disableDrift: () => Effect.void,
    reachable: Effect.succeed(true)
    })
    const EventsLive = layer({ pollInterval: "5 seconds", maxTracked: 100 }).pipe(Layer.provide(FakeTemporal))

    return Effect.gen(function*() {
      const events = yield* DeploymentEvents
      // `runCollect` is unbounded — it only completes if the stream terminates,
      // which is the property under test.
      const collector = yield* events.watch("wf1").pipe(Stream.runCollect, Effect.forkChild)

      yield* TestClock.adjust("1 second") // watcher emits its current state (50%, running)
      // The deployment finishes: it leaves the running set and its final state
      // carries an outcome.
      current = { ...state(100, "done"), outcome: "Succeeded" }
      running = []
      yield* TestClock.adjust("5 seconds") // tick publishes the terminal state → stream closes

      const collected = yield* Fiber.join(collector)
      expect(collected.at(-1)?.outcome).toBe("Succeeded")
    }).pipe(Effect.provide(EventsLive))
  })
})
