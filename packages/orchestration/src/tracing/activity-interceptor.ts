import { AsyncLocalStorage } from "node:async_hooks"
import type { ActivityExecuteInput, ActivityInterceptors } from "@temporalio/worker"
import { Tracer } from "effect"
import { decodeHeaderPayload, parseTraceparent, TRACEPARENT_HEADER_KEY } from "./traceparent.ts"

/**
 * Worker-side half of D24: decodes the `traceparent` header (forwarded by
 * `workflow-interceptors.ts`) and makes it available to the activity body as
 * an `Effect.ExternalSpan`, replacing the old runId-derived synthetic root
 * (voie B, N2) with the client's real trace.
 */
const storage = new AsyncLocalStorage<Tracer.ExternalSpan | undefined>()

/** Read from inside the running activity's Effect program. */
export const currentActivityTraceParent = (): Tracer.ExternalSpan | undefined => storage.getStore()

export const activityInterceptors = (): ActivityInterceptors => ({
  inbound: {
    execute: (input: ActivityExecuteInput, next: (input: ActivityExecuteInput) => Promise<unknown>) => {
      const raw = decodeHeaderPayload(input.headers[TRACEPARENT_HEADER_KEY])
      const parsed = raw === undefined ? undefined : parseTraceparent(raw)
      const span = parsed === undefined ? undefined : Tracer.externalSpan(parsed)
      return storage.run(span, () => next(input))
    }
  }
})
