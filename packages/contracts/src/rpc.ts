import { Schema } from "effect"
import { Rpc, RpcGroup } from "effect/unstable/rpc"
import { DeploymentState } from "./deployment.ts"

/**
 * The real-time RPC contract, shared by the control plane (server) and the CLI
 * (client), just like `FluxApi` for the request/response side (N3).
 *
 * `WatchDeployment` is a *streaming* RPC: one call yields a `Stream` of the
 * deployment's states — its current state, then every change — carried over a
 * websocket. This is the push channel a Temporal `Query` can't provide and the
 * transport behind `flux status --watch`.
 */
export const DeploymentRpcs = RpcGroup.make(
  Rpc.make("WatchDeployment", {
    payload: { workflowId: Schema.String },
    success: DeploymentState,
    stream: true
  })
)
