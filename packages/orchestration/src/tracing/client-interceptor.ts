import { AsyncLocalStorage } from "node:async_hooks"
import type { WorkflowClientInterceptor, WorkflowStartInput, WorkflowStartOutput } from "@temporalio/client"
import { Effect, Option } from "effect"
import { encodeHeaderPayload, formatTraceparent, TRACEPARENT_HEADER_KEY } from "./traceparent.ts"

/**
 * Client-side half of D24. The interceptor itself is an ordinary async
 * function registered once on the long-lived `Client` — it has no ambient
 * Effect context of its own, so it reads the traceparent from
 * `AsyncLocalStorage` instead. `withClientTraceContext` is the bridge: it
 * reads the *caller's* current Effect span and runs the wrapped effect inside
 * `storage.run(...)`, so the interceptor (invoked synchronously by the SDK as
 * part of that same call) sees it — the same `Effect.callback` bridge shape
 * `payload-codec.ts` uses for `node:zlib`, one layer up.
 */
const storage = new AsyncLocalStorage<string | undefined>()

export const traceparentClientInterceptor: WorkflowClientInterceptor = {
  async startWithDetails(
    input: WorkflowStartInput,
    next: (input: WorkflowStartInput) => Promise<WorkflowStartOutput>
  ): Promise<WorkflowStartOutput> {
    const traceparent = storage.getStore()
    if (traceparent === undefined) return next(input)
    return next({ ...input, headers: { ...input.headers, [TRACEPARENT_HEADER_KEY]: encodeHeaderPayload(traceparent) } })
  }
}

/**
 * Wrap the Promise thunk that calls the Temporal client so its current Effect
 * span reaches the workflow it starts. Takes a thunk, not an arbitrary
 * `Effect` — `Effect.callback`'s `resume(effect)` hands the wrapped effect to
 * Effect's own fiber scheduler, which does not preserve `AsyncLocalStorage`
 * context across that hop (found empirically: the interceptor saw `undefined`
 * even though the store held the right value one line above `resume`). Calling
 * `storage.run` directly around the thunk keeps the whole chain — including
 * the SDK's internal Promise continuations that invoke the interceptor — on
 * the same async lineage ALS actually tracks.
 */
export const withClientTraceContext = <A>(thunk: () => Promise<A>): Effect.Effect<A> =>
  Effect.gen(function*() {
    const span = yield* Effect.option(Effect.currentSpan)
    const traceparent = Option.match(span, {
      onNone: () => undefined,
      onSome: (s) => formatTraceparent(s.traceId, s.spanId)
    })
    return yield* Effect.promise(() => storage.run(traceparent, thunk))
  })
