import { Effect, Layer } from "effect"
import { HttpRouter } from "effect/unstable/http"
import { HttpApiBuilder, HttpApiScalar } from "effect/unstable/httpapi"
import { RpcSerialization, RpcServer } from "effect/unstable/rpc"
import { NodeHttpServer } from "@effect/platform-node"
import { DeploymentRpcs, FluxApi } from "@flux/contracts"
import { createServer } from "node:http"
import { DeploymentEvents } from "../deployment-events.ts"
import { DeploymentsHandlers, StatsHandlers } from "./handlers.ts"

/**
 * The HTTP server layer: the declarative `FluxApi` wired to its handlers, a
 * Scalar docs page (`/docs`) served from the generated OpenAPI, and the
 * websocket RPC endpoint (`/rpc`) that streams `WatchDeployment`. All three
 * register routes into one `HttpRouter`; `HttpRouter.serve` runs it on a single
 * Node server. The composed layer requires `TemporalClient` (HTTP handlers) and
 * `DeploymentEvents` (RPC handler) — the composition root provides both.
 */
const ApiLive = HttpApiBuilder.layer(FluxApi).pipe(
  Layer.provide(DeploymentsHandlers),
  Layer.provide(StatsHandlers)
)

const DocsLive = HttpApiScalar.layer(FluxApi, { path: "/docs" })

// One streaming handler: a `watch` call becomes the deployment's live state stream.
const RpcHandlers = DeploymentRpcs.toLayer(
  Effect.gen(function*() {
    const events = yield* DeploymentEvents
    return { WatchDeployment: ({ workflowId }) => events.watch(workflowId) }
  })
)

const RpcLive = RpcServer.layerHttp({ group: DeploymentRpcs, path: "/rpc", protocol: "websocket" }).pipe(
  Layer.provide(RpcHandlers),
  Layer.provide(RpcSerialization.layerJson)
)

const AppLive = Layer.mergeAll(ApiLive, DocsLive, RpcLive)

export const serverLayer = (options: { readonly port: number }) =>
  HttpRouter.serve(AppLive).pipe(
    Layer.provide(NodeHttpServer.layer(() => createServer(), { port: options.port }))
  )
