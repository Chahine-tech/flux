import { Effect } from "effect"
import { NodeRuntime, NodeServices } from "@effect/platform-node"
import { Command } from "effect/unstable/cli"
import { deploy } from "./commands/deploy.ts"
import { abort, approve } from "./commands/signals.ts"
import { status } from "./commands/status.ts"

/**
 * flux CLI — effect/unstable/cli.
 *
 * N0 runs in `direct` mode (embedded Temporal client). A `remote` mode (RPC to
 * the control plane) with runtime Layer selection arrives at N3.
 */
const flux = Command.make("flux").pipe(
  Command.withDescription("Progressive deployment orchestrator"),
  Command.withSubcommands([deploy, approve, abort, status])
)

const run = Command.run(flux, { version: "0.0.0" })

NodeRuntime.runMain(run.pipe(Effect.provide(NodeServices.layer)))
