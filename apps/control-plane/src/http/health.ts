import { Effect, Schema } from "effect"
import { HttpApi, HttpApiBuilder, HttpApiEndpoint, HttpApiGroup } from "effect/unstable/httpapi"
import { TemporalClient } from "../temporal-client.ts"

/**
 * Liveness and readiness, as two different questions.
 *
 * A separate `HttpApi` from `FluxApi` for the same reason the codec server is:
 * a kubelet does not carry a bearer token, so probes must not sit behind the
 * auth middleware. They expose nothing an unauthenticated caller could not
 * already learn by watching the port.
 *
 * **`/health/live` deliberately ignores Temporal.** Restarting the control
 * plane does not fix a Temporal outage, so tying liveness to it would turn one
 * outage into a restart loop that makes recovery slower — and would throw away
 * the in-memory admission state (D41) on every probe failure. Liveness answers
 * "is this process still serving", which is exactly what a restart can fix.
 *
 * **`/health/ready` does depend on Temporal**, because a control plane that
 * cannot reach it can accept a request and do nothing useful with it. Being
 * pulled from a Service's endpoints is the right response to that, and it is
 * reversible without losing state.
 */

const Live = Schema.Struct({ status: Schema.Literal("live") })
const Ready = Schema.Struct({ status: Schema.Literal("ready") })

/** Temporal is unreachable, or the namespace flux works in is not registered. */
export class NotReady extends Schema.TaggedError<NotReady>()(
  "NotReady",
  { reason: Schema.String },
  { httpApiStatus: 503 }
) {}

const health = HttpApiGroup.make("health")
  .add(HttpApiEndpoint.get("live", "/health/live", { success: Live }))
  .add(HttpApiEndpoint.get("ready", "/health/ready", { success: Ready, error: [NotReady] }))

export const HealthApi = HttpApi.make("flux-health").add(health)

export const HealthHandlers = HttpApiBuilder.group(HealthApi, "health", (handlers) =>
  handlers
    .handle("live", () => Effect.succeed({ status: "live" as const }))
    .handle("ready", () =>
      Effect.gen(function*() {
        const temporal = yield* TemporalClient
        return (yield* temporal.reachable)
          ? { status: "ready" as const }
          : yield* new NotReady({ reason: "Temporal is unreachable or its namespace is not registered" })
      })))
