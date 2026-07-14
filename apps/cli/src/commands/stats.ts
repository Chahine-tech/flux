import { Console, Effect } from "effect"
import { Command, Flag } from "effect/unstable/cli"
import { clientLayer, makeClient } from "../control-plane.ts"

/**
 * `flux stats` — per-service aggregates from the control plane's CQRS read
 * model (N3/D12): the questions Temporal visibility can't answer (rollback
 * rate, mean canary duration).
 */
export const stats = Command.make("stats", {
  controlPlane: Flag.string("control-plane").pipe(
    Flag.withDefault("http://localhost:8080"),
    Flag.withDescription("Control plane base URL")
  )
}, (config) =>
  Effect.gen(function*() {
    const client = yield* makeClient(config.controlPlane)
    const { services } = yield* client.stats.stats()

    if (services.length === 0) {
      return yield* Console.log("[flux] no finished deployments yet")
    }
    for (const s of services) {
      const rate = `${Math.round(s.rollbackRate * 100)}%`
      const mean = `${Math.round(s.meanDurationMs / 1000)}s`
      yield* Console.log(
        `[flux] ${s.service} — ${s.total} deployments: ${s.succeeded} ok, ${s.rolledBack} rolled back, ` +
          `${s.aborted} aborted, ${s.failed} failed (rollback rate ${rate}, mean ${mean})`
      )
    }
  }).pipe(Effect.provide(clientLayer)))
