import { Layer } from "effect"
import { RpcClient, RpcSerialization } from "effect/unstable/rpc"
import { Socket } from "effect/unstable/socket"

/**
 * Client transport for the control plane's streaming RPC (N3.5).
 *
 * `flux status --watch` opens a websocket to the control plane's `/rpc` endpoint
 * and consumes `WatchDeployment` as a `Stream`. The serialization (`layerJson`)
 * must match the server's; the global `WebSocket` (Node 24) backs the socket.
 */
export const clientProtocol = (websocketUrl: string): Layer.Layer<RpcClient.Protocol> =>
  RpcClient.layerProtocolSocket().pipe(
    Layer.provide(Socket.layerWebSocket(websocketUrl)),
    Layer.provide(Socket.layerWebSocketConstructorGlobal),
    Layer.provide(RpcSerialization.layerJson)
  )

/** Turn a control-plane base URL (`http://host:port`) into its `/rpc` websocket URL. */
export const websocketUrl = (baseUrl: string): string => `${baseUrl.replace(/^http/, "ws")}/rpc`
