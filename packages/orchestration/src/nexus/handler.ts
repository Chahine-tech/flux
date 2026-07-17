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
      workflowId: `nexus-deploy-${input.service}-${Date.now()}`,
      args: [input]
    }))
})
