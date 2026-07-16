import type {
  ActivityInput,
  WorkflowExecuteInput,
  WorkflowInboundCallsInterceptor,
  WorkflowInterceptors,
  WorkflowOutboundCallsInterceptor
} from "@temporalio/workflow"
import { TRACEPARENT_HEADER_KEY } from "./traceparent.ts"

/**
 * Workflow-side half of D24: forwards the `traceparent` header the client set
 * on `execute` onto every activity the workflow schedules. Bundled into the
 * deterministic VM (`workflowInterceptorModules`), so — same discipline as
 * `deployment.workflow.ts` (D6) — this file must never import `effect`. It
 * only copies an opaque `Payload` through; no decoding needed here.
 *
 * Found empirically, not assumed: Temporal does **not** propagate a
 * workflow's inbound headers to the activities it schedules on its own — a
 * workflow-side interceptor genuinely has to do this hop by hand.
 */
class TraceparentPropagation implements WorkflowInboundCallsInterceptor, WorkflowOutboundCallsInterceptor {
  private traceparent: ActivityInput["headers"][string] | undefined

  execute(input: WorkflowExecuteInput, next: (input: WorkflowExecuteInput) => Promise<unknown>): Promise<unknown> {
    this.traceparent = input.headers[TRACEPARENT_HEADER_KEY]
    return next(input)
  }

  scheduleActivity(input: ActivityInput, next: (input: ActivityInput) => Promise<unknown>): Promise<unknown> {
    if (this.traceparent === undefined) return next(input)
    return next({ ...input, headers: { ...input.headers, [TRACEPARENT_HEADER_KEY]: this.traceparent } })
  }
}

export const interceptors = (): WorkflowInterceptors => {
  const shared = new TraceparentPropagation()
  return { inbound: [shared], outbound: [shared] }
}
