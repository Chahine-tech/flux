import { Console, Duration, Effect, Option, Schema } from "effect"
import { Command, Flag } from "effect/unstable/cli"
import { DurationFromShorthand } from "@flux/domain"
import { clientLayer, makeClient } from "../control-plane.ts"

/**
 * `flux drift` — turn drift detection on (or off, with `--off`) for a service
 * (N4/D17). On enable, the control plane creates a Temporal Schedule that
 * periodically checks the router really routes `version` at 100% and reconciles
 * it if it has drifted; `--off` deletes that Schedule (idempotent).
 */
export const drift = Command.make("drift", {
  service: Flag.string("service").pipe(Flag.withDescription("Service name")),
  version: Flag.string("version").pipe(
    Flag.optional,
    Flag.withDescription("Version that should get 100% of traffic (required unless --off)")
  ),
  off: Flag.boolean("off").pipe(Flag.withDescription("Disable drift detection for the service")),
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
    const client = yield* makeClient(config.controlPlane)

    if (config.off) {
      yield* client.deployments.disableDrift({ params: { service: config.service } })
      yield* Console.log(`[flux] drift detection off for ${config.service}`)
      return
    }

    if (Option.isNone(config.version)) {
      return yield* Console.error("[flux] --version is required to enable drift detection")
    }
    const version = config.version.value
    const every = yield* Schema.decodeUnknownEffect(DurationFromShorthand)(config.every)
    const { scheduleId } = yield* client.deployments.enableDrift({
      payload: { service: config.service, version, everyMs: Duration.toMillis(every) }
    })
    yield* Console.log(`[flux] drift detection on for ${config.service}@${version} — schedule ${scheduleId}`)
  }).pipe(Effect.provide(clientLayer)))
