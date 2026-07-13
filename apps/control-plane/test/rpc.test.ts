import { describe, it } from "@effect/vitest"
import { Effect, Layer, Stream } from "effect"
import { RpcTest } from "effect/unstable/rpc"
import { DeploymentRpcs } from "@flux/contracts"
import type { DeploymentState } from "@flux/contracts"
import { expect } from "vitest"
import { DeploymentEvents } from "../src/deployment-events.ts"

/**
 * Exercises the streaming RPC end to end in-process (no websocket): a client
 * call to `WatchDeployment` runs the real handler, which pulls from
 * `DeploymentEvents`, and the emitted `DeploymentState`s round-trip through the
 * group's success schema back to the client as a `Stream`.
 */

const state = (currentPercent: number): DeploymentState => ({
  phase: "monitoring",
  service: "api",
  version: "v2",
  currentPercent,
  stepIndex: 0,
  totalSteps: 3
})

const MockEvents = Layer.succeed(DeploymentEvents, {
  watch: () => Stream.make(state(10), state(30), state(50))
})

const Handlers = DeploymentRpcs.toLayer(
  Effect.gen(function*() {
    const events = yield* DeploymentEvents
    return { WatchDeployment: ({ workflowId }) => events.watch(workflowId) }
  })
).pipe(Layer.provide(MockEvents))

describe("deployment RPC", () => {
  it.effect("WatchDeployment streams the deployment's states to the client", () =>
    Effect.gen(function*() {
      const client = yield* RpcTest.makeClient(DeploymentRpcs)
      const received = yield* client.WatchDeployment({ workflowId: "wf1" }).pipe(Stream.runCollect)
      expect(received.map((s) => s.currentPercent)).toEqual([10, 30, 50])
    }).pipe(Effect.provide(Handlers), Effect.scoped))
})
