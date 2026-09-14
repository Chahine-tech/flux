import { Duration, Effect, Layer } from "effect"
import { HttpClient, HttpClientError } from "effect/unstable/http"
import { NodeHttpClient } from "@effect/platform-node"

/**
 * No outbound HTTP call may hang, and the reason is a k3d experiment (D50).
 *
 * Blackholing Prometheus with a NetworkPolicy produced no error at all: the
 * `monitorStep` activity simply sat in flight for minutes, long past its 30
 * second `heartbeatTimeout`. The heartbeat could not catch it, because it is a
 * concurrent daemon that keeps proving the *process* alive while the work makes
 * no progress. With nothing bounding the request either, the only ceiling left
 * was the activity's one hour `startToCloseTimeout`, so a partitioned
 * Prometheus could hold a canary at 10% of traffic for an hour.
 *
 * The bound belongs here rather than in each adapter: it is one property of
 * the process ("no call to anything waits forever"), and seven adapters make
 * outbound calls. The timeout becomes a `TransportError`, which is what it
 * actually is, so the client's error type is unchanged and every adapter's
 * existing mapping (`MetricsUnavailable` and the rest) keeps working untouched.
 *
 * An adapter that genuinely needs longer overrides it locally; the AI
 * postmortem is the candidate, and it is off the deployment's critical path.
 */
/**
 * Read when the layer is built, not when this module is imported. A top-level
 * `const` froze the value at import, which made the override untestable and,
 * worse, silently ignored anything set after the first import anywhere in the
 * process.
 */
const requestTimeout = (): Duration.Duration =>
  Duration.seconds(Number(process.env.FLUX_HTTP_TIMEOUT_SECONDS ?? 15))

export const BoundedHttpClient = Layer.effect(
  HttpClient.HttpClient,
  Effect.map(HttpClient.HttpClient, (client) => {
    const timeout = requestTimeout()
    return HttpClient.transform(client, (effect, request) =>
      Effect.timeoutOrElse(effect, {
        duration: timeout,
        orElse: () =>
          Effect.fail(
            new HttpClientError.HttpClientError({
              reason: new HttpClientError.TransportError({
                request,
                cause: new Error(`no response within ${Duration.toSeconds(timeout)}s`)
              })
            })
          )
      }))
  })
).pipe(
  Layer.provide(NodeHttpClient.layerUndici),
  // `http.client GET` three times in a row says the protocol and hides the
  // point. The host is what tells Prometheus from Caddy at a glance, and
  // Effect exposes the name as a context reference rather than making this a
  // fork of the client.
  //
  // After the `provide`, not before: the reference has to end up in this
  // layer's *output* so a request fiber can read it, and a `provide` applied
  // afterwards keeps only its own output.
  Layer.provideMerge(
    Layer.succeed(HttpClient.SpanNameGenerator, (request) => {
      const host = URL.parse(request.url)?.host
      return host === undefined ? `http.client ${request.method}` : `http.client ${request.method} ${host}`
    })
  )
)
