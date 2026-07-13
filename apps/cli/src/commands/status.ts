import { Console, Effect, Stream } from "effect"
import { Command, Flag } from "effect/unstable/cli"
import { RpcClient } from "effect/unstable/rpc"
import { DeploymentRpcs } from "@flux/contracts"
import type { DeploymentState } from "@flux/contracts"
import { clientProtocol, websocketUrl } from "../rpc.ts"
import { queryStatus } from "../temporal.ts"

const render = (state: DeploymentState): string => {
  const outcome = state.outcome ? ` (${state.outcome})` : ""
  return `[flux] ${state.service} ${state.version} — ${state.phase} @ ${state.currentPercent}%` +
    ` [step ${state.stepIndex + 1}/${state.totalSteps}]${outcome}`
}

// --watch: stream live state from the control plane over the RPC websocket.
const watch = (workflowId: string, controlPlane: string) =>
  Effect.gen(function*() {
    const client = yield* RpcClient.make(DeploymentRpcs)
    yield* client.WatchDeployment({ workflowId }).pipe(
      Stream.runForEach((state) => Console.log(render(state)))
    )
  }).pipe(Effect.provide(clientProtocol(websocketUrl(controlPlane))), Effect.scoped)

// Default: a single snapshot straight from the workflow's `status` query.
const once = (workflowId: string) =>
  Effect.gen(function*() {
    const state = yield* Effect.promise(() => queryStatus(workflowId))
    yield* Console.log(render(state))
  })

/**
 * `flux status` — show the canary state. Dual-mode: a one-shot Temporal query by
 * default, or a live stream from the control plane with `--watch`.
 */
export const status = Command.make("status", {
  workflowId: Flag.string("workflow-id").pipe(
    Flag.withDescription("Deployment workflow id (from `flux deploy`)")
  ),
  watch: Flag.boolean("watch").pipe(
    Flag.withDescription("Stream live updates from the control plane instead of a single snapshot")
  ),
  controlPlane: Flag.string("control-plane").pipe(
    Flag.withDefault("http://localhost:8080"),
    Flag.withDescription("Control plane base URL (used with --watch)")
  )
}, (config) => config.watch ? watch(config.workflowId, config.controlPlane) : once(config.workflowId))
