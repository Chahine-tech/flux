import { Duration, Effect, Layer } from "effect"
import { Otlp } from "effect/unstable/observability"
import { NodeHttpClient } from "@effect/platform-node"

/**
 * The CLI's root span, and the reason the trace reaches the control plane at
 * all.
 *
 * D24 carried a `traceparent` from the CLI through Temporal to every activity,
 * and left the CLI to control-plane HTTP hop open, describing it as a separate
 * propagation problem. It is not one. Effect v4 propagates both halves already:
 * `HttpClient` sets `traceparent` on every outgoing request from the current
 * span, and the server's `HttpMiddleware.tracer` parents its request span from
 * the incoming headers. What was missing was a span to propagate. The CLI
 * created none, so the client had nothing to put in the header.
 *
 * So this is a root span per command, and nothing more.
 *
 * `Effect.withSpan` is enough on its own for the hop: the client span the
 * `HttpClient` creates inherits this trace whatever tracer is installed, since
 * even the native tracer generates real ids. The OTLP layer only decides
 * whether anyone can *see* the trace. Unset `OTLP_ENDPOINT` and the trace is
 * still propagated, just not recorded, which is why the gate is on the layer
 * and never on the span.
 *
 * The gate is not a nicety either, and it is where the CLI has to differ from
 * the worker. The worker's layer is unconditional with a `localhost:4318`
 * default, which is free for a process that runs for days. Here the exporter
 * flushes during scope finalization, so a collector that is not listening
 * costs the full shutdown timeout on the way out. Measured against a dead
 * endpoint: 3054 ms per invocation on the 3 second default, 1054 ms on the one
 * second set here, and 1 ms with the layer absent. Every `flux deploy` would
 * pay it. Hence both the gate and the shorter timeout, since a CLI that cannot
 * reach its collector should give up long before a server would.
 */
const tracingLayer = (): Layer.Layer<never> => {
  const endpoint = process.env.OTLP_ENDPOINT
  if (endpoint === undefined) return Layer.empty
  return Otlp.layerJson({
    baseUrl: endpoint,
    resource: { serviceName: "flux-cli" },
    shutdownTimeout: Duration.seconds(1)
  }).pipe(Layer.provide(NodeHttpClient.layerUndici))
}

/**
 * Wrap a command in its root span. Applied outermost, after the command's own
 * error handling, so the span covers the whole invocation rather than stopping
 * at the first `catchTag`.
 *
 * Spans are flushed during scope finalization (up to the exporter's shutdown
 * timeout), which is what makes this work in a process that exits immediately:
 * `NodeRuntime.runMain` finalizes the layer on the way out.
 */
export const tracedCommand = (name: string) => <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
  effect.pipe(
    Effect.withSpan(`flux ${name}`),
    Effect.provide(tracingLayer())
  )
