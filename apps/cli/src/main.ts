import { Effect } from "effect"
import { NodeRuntime, NodeServices } from "@effect/platform-node"
import { Command } from "effect/unstable/cli"
import { deploy } from "./commands/deploy.ts"
import { deployMulti } from "./commands/deploy-multi.ts"
import { drift } from "./commands/drift.ts"
import { history } from "./commands/history.ts"
import { abort, approve } from "./commands/signals.ts"
import { stats } from "./commands/stats.ts"
import { status } from "./commands/status.ts"

/**
 * flux CLI — effect/unstable/cli.
 *
 * Writes (`deploy`, `deploy-multi`) go through the control plane's HTTP API so
 * they pass admission control; reads (`status`, `history`) query Temporal
 * directly. `status --watch` streams over the control plane's websocket.
 */
const flux = Command.make("flux").pipe(
  Command.withDescription("Progressive deployment orchestrator"),
  Command.withSubcommands([deploy, deployMulti, drift, approve, abort, status, stats, history])
)

const run = Command.run(flux, { version: "0.0.0" })

NodeRuntime.runMain(run.pipe(Effect.provide(NodeServices.layer)))
