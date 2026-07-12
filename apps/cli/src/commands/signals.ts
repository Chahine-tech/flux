import { Console, Effect } from "effect"
import { Command, Flag } from "effect/unstable/cli"
import { updateDeployment } from "../temporal.ts"

const workflowIdFlag = Flag.string("workflow-id").pipe(
  Flag.withDescription("Deployment workflow id (from `flux deploy`)")
)

/** `flux approve` — approve advancing past a manual-approval gate (validated Update). */
export const approve = Command.make("approve", { workflowId: workflowIdFlag }, (config) =>
  Effect.gen(function*() {
    yield* Effect.promise(() => updateDeployment(config.workflowId, "approve"))
    yield* Console.log(`[flux] approved ${config.workflowId}`)
  }))

/** `flux abort` — abort an in-flight deployment (validated Update). */
export const abort = Command.make("abort", { workflowId: workflowIdFlag }, (config) =>
  Effect.gen(function*() {
    yield* Effect.promise(() => updateDeployment(config.workflowId, "abort"))
    yield* Console.log(`[flux] aborted ${config.workflowId}`)
  }))
