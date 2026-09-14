import { Context, type Duration, type Fiber, Layer, Logger, Option, Tracer } from "effect"
import { Otlp } from "effect/unstable/observability"
import { treeLayer } from "./tree.ts"
import { HttpClient } from "effect/unstable/http"

/**
 * How a flux process reports on itself: the format it writes its logs in, and
 * the resource attributes its spans carry.
 *
 * Both were unset. The apps used Effect's default logger everywhere and sent a
 * `serviceName` and nothing else with their spans, which was survivable while
 * logs were sparse and there was one environment to look at. Neither is true
 * any more: D24's deferred half now routes every workflow's log lines through
 * Effect as well, and the same three services run under compose and in a
 * cluster.
 */

/**
 * JSON when something is parsing, logfmt when someone is reading.
 *
 * The decision is made by whether stdout is a terminal rather than by
 * `NODE_ENV`, because the real question is "is a human looking at this", and a
 * container answers it correctly without being told. `docker run -it` gets the
 * readable form, which is the right answer rather than an accident.
 *
 * Both formats keep log annotations, which was the thing to check rather than
 * assume: they are what carries a workflow's `workflowId` and `sdkComponent`
 * next to the activity lines of the same deployment, and a format that dropped
 * them would have quietly undone that work.
 *
 * **Two loggers, not one, and the second is a regression being undone.**
 * `Logger.tracerLogger` records every log message as an event on the current
 * span, and the docs are explicit that it "is included in the default set of
 * loggers for all Effect programs [...] unless you override the default
 * loggers". `Logger.layer` overrides them. So the first version of this file
 * silently stopped log lines from appearing inside their spans in Jaeger, which
 * is the sort of loss nobody notices, because the thing that disappears is a
 * thing you have to go and look for.
 */
const currentSpan = (fiber: Fiber.Fiber<unknown, unknown>): Tracer.AnySpan | undefined =>
  Option.getOrUndefined(Context.getOption(fiber.context, Tracer.ParentSpan))

/**
 * The other direction of the same link: the trace ids on the log line.
 *
 * `tracerLogger` lets you go from a trace to its logs. Nothing let you go the
 * other way, which is the direction you actually need at three in the morning:
 * you have a log line, and you want the trace it belongs to. Verified absent
 * before building it, with and without a real tracer installed, because
 * `formatJson` emits a `spans` field that looks like it might hold this and
 * does not (it is `withLogSpan`, a different thing entirely).
 *
 * The record is built by the stock formatter and then added to, rather than
 * reimplemented, so annotations and levels keep behaving exactly as they do
 * everywhere else.
 */
const formatted = (json: boolean): Logger.Logger<unknown, void> =>
  Logger.make((options) => {
    const span = currentSpan(options.fiber)
    if (json) {
      const record = Logger.formatStructured.log(options) as Record<string, unknown>
      const line = span === undefined ? record : { ...record, traceId: span.traceId, spanId: span.spanId }
      globalThis.console.log(JSON.stringify(line))
      return
    }
    const base = Logger.formatLogFmt.log(options)
    globalThis.console.log(
      span === undefined ? base : `${base} traceId=${span.traceId} spanId=${span.spanId}`
    )
  })

export const loggerLayer = (): Layer.Layer<never> => {
  // `FLUX_TRACE_CONSOLE=1` swaps the line logger for the span tree. One
  // decision in one place rather than two layers racing to provide the same
  // service, and it has to be exclusive: the tree already contains the log
  // lines, so keeping both would print everything twice.
  if (process.env.FLUX_TRACE_CONSOLE === "1") return treeLayer(currentSpan)
  const explicit = process.env.FLUX_LOG_FORMAT
  const json = explicit === undefined ? process.stdout.isTTY !== true : explicit === "json"
  return Logger.layer([formatted(json), Logger.tracerLogger])
}

/**
 * The resource every span is attributed to.
 *
 * `serviceVersion` is omitted rather than defaulted, and that is deliberate.
 * Every package here is `0.0.0`, so emitting it would attach a number that
 * distinguishes nothing to every span, which is worse than no attribute at all:
 * an absent field reads as "unknown", a wrong one reads as an answer. Set
 * `FLUX_VERSION` at image build time (a git sha is the useful value) and it
 * appears.
 *
 * The environment is what makes a trace searchable once the same three services
 * run in two places, which is exactly where flux is since D46.
 */
export const resource = (serviceName: string): {
  readonly serviceName: string
  readonly serviceVersion?: string
  readonly attributes: Record<string, unknown>
} => {
  const version = process.env.FLUX_VERSION
  return {
    serviceName,
    ...(version === undefined || version === "" ? {} : { serviceVersion: version }),
    attributes: { "deployment.environment.name": process.env.FLUX_ENV ?? "development" }
  }
}

/**
 * Export spans, if and only if somewhere was named to send them to.
 *
 * The worker and the control plane used to default to `http://localhost:4318`,
 * which is right on a developer's machine and wrong in every container: inside
 * a pod that address is the pod itself, where nothing listens. So the whole
 * trace chain D24 and D47 built was silently off in the cluster D46 put flux
 * in, quietly retrying an export that could never land. The CLI already gated
 * on the variable; this makes the rule the same for all three, which is also
 * the only rule simple enough to state: name an endpoint and traces are
 * exported, name none and they are not.
 *
 * Propagation is unaffected either way. The `traceparent` still travels, since
 * even the native tracer generates real ids (D47); what an endpoint decides is
 * whether anyone can see the result.
 */
export const tracingLayer = (
  serviceName: string,
  options?: { readonly shutdownTimeout?: Duration.Duration }
): Layer.Layer<never, never, HttpClient.HttpClient> => {
  const endpoint = process.env.OTLP_ENDPOINT
  if (endpoint === undefined || endpoint === "") return Layer.empty
  return Otlp.layerJson({
    baseUrl: endpoint,
    resource: resource(serviceName),
    ...(options?.shutdownTimeout === undefined ? {} : { shutdownTimeout: options.shutdownTimeout })
  })
}
