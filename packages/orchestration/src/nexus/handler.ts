import { startWorkflow, WorkflowRunOperationHandler } from "@temporalio/nexus"
import { serviceHandler } from "nexus-rpc"
import type { DeploymentInput, DeploymentResult } from "../deployment-input.ts"
import type { deploymentWorkflow } from "../workflows/deployment.workflow.ts"
import { DeployService } from "./service.ts"

/**
 * Worker-side half of D25: the operation is backed by the existing
 * `deploymentWorkflow` — no new workflow logic, the platform namespace runs
 * exactly the same canary any direct caller would trigger. `startWorkflow`
 * defaults to the current worker's task queue (the one polling Nexus tasks),
 * so the platform worker that registers this handler must also run
 * `deploymentWorkflow` and the four activities on that same queue.
 *
 * `import type` on `deploymentWorkflow` only pulls its signature for
 * `startWorkflow`'s generic inference — the actual dispatch goes through the
 * string workflow type name, like every other cross-queue Temporal call.
 */
export const DeployServiceHandler = serviceHandler(DeployService, {
  runCanary: new WorkflowRunOperationHandler<DeploymentInput, DeploymentResult>(async (ctx, input) =>
    startWorkflow<typeof deploymentWorkflow>(ctx, "deploymentWorkflow", {
      // Business-meaningful, deterministic — the id is what dedupes a retried
      // Nexus start (delivery is at-least-once: a handler that started the
      // workflow but crashed before replying gets re-invoked, and a
      // `Date.now()` here would start a SECOND canary for the same
      // deployment). Same key shape as the comparison package's
      // `idempotencyKey` (D23) — the two engines agree on what identifies a
      // deployment. A re-deploy of the same service+version after completion
      // is still allowed (default workflow-id reuse policy); one while it is
      // already running is rejected, which is admission control behaving.
      workflowId: `nexus-deploy-${input.service}-${input.version}`,
      args: [input]
    }))
})
