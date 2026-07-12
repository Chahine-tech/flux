import { Console, Effect } from "effect"
import { Command, Flag } from "effect/unstable/cli"
import { listDeployments } from "../temporal.ts"

/** `flux history` — list recent deployments via advanced-visibility queries. */
export const history = Command.make("history", {
  service: Flag.string("service").pipe(
    Flag.withDefault(""),
    Flag.withDescription("Filter by service (default: all services)")
  ),
  last: Flag.integer("last").pipe(
    Flag.withDefault(10),
    Flag.withDescription("Maximum number of deployments to show")
  )
}, (config) =>
  Effect.gen(function*() {
    const rows = yield* Effect.promise(() => listDeployments(config.service, config.last))
    if (rows.length === 0) {
      yield* Console.log("[flux] no deployments found")
      return
    }
    for (const row of rows) {
      yield* Console.log(`  ${row.startTime}  ${row.status.padEnd(10)}  ${row.workflowId}`)
    }
  }))
