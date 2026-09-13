import type { ConnectionLike } from "@temporalio/client"

/**
 * A deadline on every gRPC call to Temporal. The SDK's own types explain why:
 * "It is strongly recommended to explicitly set deadlines. If no deadline is
 * set, then it is possible for the client to end up waiting forever for a
 * response."
 *
 * D52 watched that happen. With Temporal scaled to zero in a cluster, nothing
 * returned and nothing was logged: the poll tick never completed, requests were
 * never answered, and the 503 D49 designed was unreachable because no call ever
 * *failed*. A typed error only helps once the call comes back.
 *
 * `withDeadline` rather than an `Effect.timeout` laid over the top, because it
 * cancels the request instead of abandoning it while it keeps running, and the
 * `DEADLINE_EXCEEDED` it raises is an ordinary `ServiceError` that the callers'
 * existing classifiers already turn into `TemporalUnavailable`. The fix is a
 * deadline, not a new error path.
 *
 * One budget covers every call, because they are all short service RPCs
 * (start, query, describe, list, schedule); nothing here waits on a workflow to
 * finish. Read per call rather than captured once, so setting the environment
 * variable after this module is imported still takes effect.
 */
export const withDeadline = <A>(connection: ConnectionLike, fn: () => Promise<A>): Promise<A> =>
  connection.withDeadline(Date.now() + Number(process.env.TEMPORAL_CALL_TIMEOUT_MS ?? 10_000), fn)
