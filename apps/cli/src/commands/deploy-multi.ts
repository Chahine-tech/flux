import { Console, Effect, Schema } from "effect"
import { Command, Flag } from "effect/unstable/cli"
import { TriggerMultiRequest } from "@flux/contracts"
import { readFileSync } from "node:fs"
import { clientLayer, makeClient } from "../control-plane.ts"

/**
 * `flux deploy-multi` — roll one version out across several services at once
 * (N4/D13), as a parent workflow over one child per service. The rollout is
 * described by a JSON file (too much for flags); it is validated against the
 * shared contract before being sent to the control plane.
 */
export const deployMulti = Command.make("deploy-multi", {
  config: Flag.string("config").pipe(
    Flag.withDescription("Path to a JSON file: { services: [...], maxConcurrency, failFast }")
  ),
  controlPlane: Flag.string("control-plane").pipe(
    Flag.withDefault("http://localhost:8080"),
    Flag.withDescription("Control plane base URL")
  )
}, (config) =>
  Effect.gen(function*() {
    const raw = yield* Effect.try(() => JSON.parse(readFileSync(config.config, "utf8")))
    const request = yield* Schema.decodeUnknownEffect(TriggerMultiRequest)(raw)
    const client = yield* makeClient(config.controlPlane)
    const { workflowId } = yield* client.deployments.triggerMulti({ payload: request })

    yield* Console.log(
      `[flux] started multi-service rollout ${workflowId} — ${request.services.length} services` +
        ` (max ${request.maxConcurrency} at once, fail-fast ${request.failFast})`
    )
  }).pipe(Effect.provide(clientLayer)))
