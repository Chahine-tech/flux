import { Console, Effect } from "effect"
import { Command, Flag } from "effect/unstable/cli"
import { signalDeployment } from "../temporal.ts"

const workflowIdFlag = Flag.string("workflow-id").pipe(
  Flag.withDescription("Deployment workflow id (from `flux deploy`)")
)

/** `flux approve` — approve advancing past a manual-approval gate. */
export const approve = Command.make("approve", { workflowId: workflowIdFlag }, (config) =>
  Effect.gen(function*() {
    yield* Effect.promise(() => signalDeployment(config.workflowId, "approve"))
    yield* Console.log(`[flux] approved ${config.workflowId}`)
  }))

/** `flux abort` — abort an in-flight deployment. */
export const abort = Command.make("abort", { workflowId: workflowIdFlag }, (config) =>
  Effect.gen(function*() {
    yield* Effect.promise(() => signalDeployment(config.workflowId, "abort"))
    yield* Console.log(`[flux] aborted ${config.workflowId}`)
  }))
