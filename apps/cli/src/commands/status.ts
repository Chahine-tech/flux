import { Console, Effect } from "effect"
import { Command, Flag } from "effect/unstable/cli"
import { describeDeployment } from "../temporal.ts"

/** `flux status` — show the current status of a deployment workflow. */
export const status = Command.make("status", {
  workflowId: Flag.string("workflow-id").pipe(
    Flag.withDescription("Deployment workflow id (from `flux deploy`)")
  )
}, (config) =>
  Effect.gen(function*() {
    const status = yield* Effect.promise(() => describeDeployment(config.workflowId))
    yield* Console.log(`[flux] ${config.workflowId}: ${status}`)
  }))
