import { Console, Duration, Effect, Schema } from "effect"
import { Command, Flag } from "effect/unstable/cli"
import { DurationFromShorthand } from "@flux/domain"
import { clientLayer, makeClient } from "../control-plane.ts"

/**
 * `flux drift` — turn on drift detection for a service (N4/D17). The control
 * plane creates a Temporal Schedule that periodically checks the router really
 * routes `version` at 100% and reconciles it if it has drifted.
 */
export const drift = Command.make("drift", {
  service: Flag.string("service").pipe(Flag.withDescription("Service name")),
  version: Flag.string("version").pipe(Flag.withDescription("Version that should get 100% of traffic")),
  every: Flag.string("every").pipe(
    Flag.withDefault("1m"),
    Flag.withDescription("How often to check (e.g. 30s, 5m)")
  ),
  controlPlane: Flag.string("control-plane").pipe(
    Flag.withDefault("http://localhost:8080"),
    Flag.withDescription("Control plane base URL")
  )
}, (config) =>
  Effect.gen(function*() {
    const every = yield* Schema.decodeUnknownEffect(DurationFromShorthand)(config.every)
    const client = yield* makeClient(config.controlPlane)
    const { scheduleId } = yield* client.deployments.enableDrift({
      payload: { service: config.service, version: config.version, everyMs: Duration.toMillis(every) }
    })
    yield* Console.log(`[flux] drift detection on for ${config.service}@${config.version} — schedule ${scheduleId}`)
  }).pipe(Effect.provide(clientLayer)))
