import { Effect, Layer, Option, Stream, Tracer } from "effect"
import { FetchHttpClient, HttpClient, HttpRouter, HttpServer } from "effect/unstable/http"
import { HttpApiBuilder, HttpApiClient } from "effect/unstable/httpapi"
import { DeploymentNotFound, FluxApi } from "@flux/contracts"
import { Otlp } from "effect/unstable/observability"
import { createServer } from "node:net"
import { describe, expect, it } from "vitest"
import * as Admission from "../src/admission.ts"
import { DeploymentEvents } from "../src/deployment-events.ts"
import * as Auth from "../src/http/auth.ts"
import { DeploymentsHandlers, StatsHandlers } from "../src/http/handlers.ts"
import { serverLayer } from "../src/http/server.ts"
import { ReadModel } from "../src/read-model.ts"
import { TemporalClient } from "../src/temporal-client.ts"

/**
 * The CLI to control-plane trace hop, the last one D24 left open.
 *
 * This runs the real derived client against the real handler in one process,
 * so what is asserted is the hop itself rather than its two halves separately:
 * a root span on the caller, a request over HTTP, and a server span that turns
 * out to belong to the same trace.
 *
 * The client is built with `HttpApiClient.make(FluxApi, ...)`, which is exactly
 * what `apps/cli/src/control-plane.ts` builds. Importing the CLI's own
 * `makeClient` would mean a control-plane test depending on the CLI package, so
 * the client machinery is reproduced rather than imported; the bearer header is
 * the only thing the CLI adds on top, and it has no bearing on tracing.
 *
 * The tracers are capturing rather than exporting. It also matters that they
 * exist at all: `HttpEffect` skips the server tracer middleware entirely when
 * the installed tracer is the native one, on the grounds that its spans are
 * unobservable, so a test with no tracer would pass vacuously by never
 * creating a server span to check.
 */

const capturing = (into: Array<Tracer.Span>): Tracer.Tracer =>
  Tracer.make({
    span: (options) => {
      const span = new Tracer.NativeSpan(options)
      into.push(span)
      return span
    }
  })

const MockTemporal = Layer.succeed(TemporalClient, {
  start: (request) => Effect.succeed(`dep-${request.service}-test`),
  startMulti: () => Effect.succeed("multi"),
  status: (workflowId) => Effect.fail(new DeploymentNotFound({ workflowId })),
  list: () => Effect.succeed([]),
  listRunningIds: () => Effect.succeed([]),
  listClosed: () => Effect.succeed([]),
  approve: () => Effect.void,
  abort: () => Effect.void,
  ensureDriftSchedule: () => Effect.succeed("flux-drift-api"),
  disableDrift: () => Effect.void,
  reachable: Effect.succeed(true)
})

const MockReadModel = Layer.succeed(ReadModel, { stats: () => Effect.succeed([]) })

const trigger = {
  service: "checkout",
  version: "v2",
  previousVersion: "v1",
  strategy: { kind: "canary", steps: [{ percent: 10, monitorMs: 60_000, requiresApproval: false }] },
  rules: [{ name: "error_rate", query: "sum(rate(errors[1m]))", max: 0.01 }],
  pollIntervalMs: 5_000
} as const

/**
 * One deployment call, end to end. Returns every span both sides created, so
 * the assertions can be about identity rather than about counts.
 */
const deployThroughHttp = async (options: { readonly propagate: boolean }) => {
  const serverSpans: Array<Tracer.Span> = []
  const clientSpans: Array<Tracer.Span> = []

  const app = HttpApiBuilder.layer(FluxApi).pipe(
    Layer.provide(DeploymentsHandlers),
    Layer.provide(StatsHandlers),
    Layer.provide(Auth.layer(Option.none())),
    HttpRouter.provideRequest(Layer.mergeAll(MockTemporal, MockReadModel, Admission.layer(100))),
    Layer.provide(HttpServer.layerServices),
    // `provideMerge`, not `provide`: the tracer has to end up in the layer's
    // *output* context, because that is what `toWebHandler` provides to each
    // request fiber, and the tracer middleware reads it from there. Provided
    // as a dependency it would only be visible while building the layer, and
    // no server span gets created at all.
    Layer.provideMerge(Layer.succeed(Tracer.Tracer, capturing(serverSpans)))
  )
  const { dispose, handler } = HttpRouter.toWebHandler(app)

  // The transport: the client's `fetch` is the server's handler, so a request
  // really is serialized to headers and parsed back out of them. Nothing about
  // the trace context is handed over in memory.
  //
  // `FetchHttpClient` calls `fetch(url, init)` with a `URL`, not a `Request`,
  // so the two have to be assembled here; handing the handler the first
  // argument directly gives it an object with no `url` and it fails inside
  // `removeHost`.
  const intoHandler: typeof fetch = (input, init) => handler(new Request(input as URL, init))
  const transport = FetchHttpClient.layer.pipe(
    Layer.provide(Layer.succeed(FetchHttpClient.Fetch, intoHandler))
  )

  try {
    const result = await Effect.runPromise(
      Effect.gen(function*() {
        const client = yield* HttpApiClient.make(FluxApi, { baseUrl: "http://control-plane" })
        return yield* client.deployments.trigger({ payload: trigger })
      }).pipe(
        Effect.withSpan("flux deploy"),
        Effect.provide(transport),
        options.propagate
          ? (effect) => effect
          : Effect.provideService(HttpClient.TracerPropagationEnabled, false),
        Effect.provide(Layer.succeed(Tracer.Tracer, capturing(clientSpans)))
      )
    )
    return { result, serverSpans, clientSpans }
  } finally {
    await dispose()
  }
}

const named = (spans: ReadonlyArray<Tracer.Span>, prefix: string) => spans.find((s) => s.name.startsWith(prefix))

/** Only the three fields this test reads, out of the OTLP span JSON. */
interface OtlpSpan {
  readonly name: string
  readonly traceId: string
  readonly parentSpanId?: string
}

describe("the CLI to control-plane trace hop", () => {
  it("puts the server's request span in the caller's trace", async () => {
    const { clientSpans, result, serverSpans } = await deployThroughHttp({ propagate: true })
    expect(result.workflowId).toBe("dep-checkout-test")

    const root = named(clientSpans, "flux deploy")
    const clientCall = named(clientSpans, "http.client")
    const serverRequest = named(serverSpans, "http.server")

    // All three have to exist, or an assertion below could hold vacuously.
    expect(root, "the CLI's root span").toBeDefined()
    expect(clientCall, "the outgoing client span").toBeDefined()
    expect(serverRequest, "the server's request span").toBeDefined()

    // One trace across the process boundary.
    expect(serverRequest!.traceId).toBe(root!.traceId)

    // And parented on the client call, not merely sharing a trace id: this is
    // what makes the server span a child in the waterfall rather than a second
    // root that happens to be labelled the same.
    expect(Option.map(serverRequest!.parent, (p) => p.spanId)).toEqual(Option.some(clientCall!.spanId))
    expect(serverRequest!.kind).toBe("server")
  })

  it("starts a new trace when propagation is off, which is what proves the header does the work", async () => {
    const { clientSpans, serverSpans } = await deployThroughHttp({ propagate: false })

    const root = named(clientSpans, "flux deploy")
    const serverRequest = named(serverSpans, "http.server")
    expect(root, "the CLI's root span").toBeDefined()
    expect(serverRequest, "the server's request span").toBeDefined()

    // Same code, same call, one reference flipped: the trace breaks. Without
    // this the first test would also pass if trace ids were, say, derived from
    // something both sides compute independently.
    expect(serverRequest!.traceId).not.toBe(root!.traceId)
    expect(Option.isNone(serverRequest!.parent)).toBe(true)
  })
})

/** A port the OS just confirmed is free, so the live-server test cannot collide. */
const freePort = () =>
  new Promise<number>((resolve, reject) => {
    const probe = createServer()
    probe.once("error", reject)
    probe.listen(0, "127.0.0.1", () => {
      const address = probe.address()
      if (address === null || typeof address === "string") {
        probe.close()
        reject(new Error("no port"))
        return
      }
      const { port } = address
      probe.close(() => resolve(port))
    })
  })

/**
 * The test above proves the hop works when a tracer is installed. It does not
 * prove that the control plane installs one, because it installs its own.
 *
 * That gap is worth closing by running the real thing rather than by reasoning
 * about it, for two reasons that both point the same way. The manner of
 * providing a tracer decides whether a request fiber can see it: this very
 * file needed `Layer.provideMerge` where `Layer.provide` silently produced no
 * server span at all. And `Otlp.layerJson` is typed `Layer<never, never,
 * HttpClient>`, so reading the signature suggests it provides nothing, when in
 * fact it does `Layer.effect(Tracer.Tracer)` and the output is only erased
 * from the type because `Tracer.Tracer` is a `Context.Reference`.
 *
 * So this runs the real `serverLayer` on a real socket with the real
 * `Otlp.layerJson` from `main.ts`, and asserts on what the exporter actually
 * puts on the wire. The collector is a stub, which is the only substitution.
 */
describe("the control plane's own composition", () => {
  it("exports the request span into the trace the caller sent", async () => {
    const port = await freePort()
    const traceId = "0af7651916cd43dd8448eb211c80319c"
    const parentSpanId = "b7ad6b7169203331"

    // Stands in for the collector, and records what it was sent.
    const exported: Array<OtlpSpan> = []
    const collector: typeof fetch = async (_input, init) => {
      const payload = JSON.parse(String(init?.body)) as {
        readonly resourceSpans?: ReadonlyArray<
          { readonly scopeSpans?: ReadonlyArray<{ readonly spans?: ReadonlyArray<OtlpSpan> }> }
        >
      }
      for (const resource of payload.resourceSpans ?? []) {
        for (const scope of resource.scopeSpans ?? []) exported.push(...(scope.spans ?? []))
      }
      return new Response("{}", { status: 200, headers: { "content-type": "application/json" } })
    }

    // Verbatim from `main.ts`, which is the point of the test.
    const TracingLayer = Otlp.layerJson({
      baseUrl: "http://collector",
      resource: { serviceName: "flux-control-plane" }
    }).pipe(Layer.provide(FetchHttpClient.layer.pipe(Layer.provide(Layer.succeed(FetchHttpClient.Fetch, collector)))))

    const live = serverLayer({ port }).pipe(
      Layer.provide(Auth.layer(Option.none())),
      Layer.provide(Layer.succeed(DeploymentEvents, { watch: () => Stream.empty })),
      Layer.provide(MockReadModel),
      Layer.provide(Admission.layer(100)),
      Layer.provide(MockTemporal),
      Layer.provide(TracingLayer)
    )

    await Effect.runPromise(
      Effect.gen(function*() {
        const response = yield* Effect.promise(() =>
          fetch(`http://127.0.0.1:${port}/deployments`, {
            method: "POST",
            headers: {
              "content-type": "application/json",
              traceparent: `00-${traceId}-${parentSpanId}-01`
            },
            body: JSON.stringify(trigger)
          })
        )
        expect(response.status).toBe(200)
      }).pipe(Effect.provide(live), Effect.scoped)
    )

    // The exporter batches and flushes during scope finalization, so nothing
    // is on the wire until the layer above has been released.
    const serverRequest = exported.find((span) => span.name.startsWith("http.server"))
    expect(serverRequest, "an exported server span").toBeDefined()
    expect(serverRequest!.traceId).toBe(traceId)
    expect(serverRequest!.parentSpanId).toBe(parentSpanId)
  })
})
