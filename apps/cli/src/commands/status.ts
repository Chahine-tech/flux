import { Console, Effect } from "effect"
import { Command, Flag } from "effect/unstable/cli"
import { queryStatus } from "../temporal.ts"

/** `flux status` — show the live canary state via the workflow's query. */
export const status = Command.make("status", {
  workflowId: Flag.string("workflow-id").pipe(
    Flag.withDescription("Deployment workflow id (from `flux deploy`)")
  )
}, (config) =>
  Effect.gen(function*() {
    const state = yield* Effect.promise(() => queryStatus(config.workflowId))
    const outcome = state.outcome ? ` (${state.outcome})` : ""
    yield* Console.log(
      `[flux] ${state.service} ${state.version} — ${state.phase} @ ${state.currentPercent}%` +
        ` [step ${state.stepIndex + 1}/${state.totalSteps}]${outcome}`
    )
  }))
