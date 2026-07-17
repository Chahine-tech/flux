import * as wf from "@temporalio/workflow"
import type { DeploymentInput, DeploymentResult } from "../deployment-input.ts"
import { DeployService } from "../nexus/service.ts"

/**
 * Caller-side half of D25 (N9): a workflow in a tenant namespace triggers a
 * canary in the platform namespace through the Nexus endpoint, with no
 * access to that namespace otherwise. Plain deterministic TypeScript, same
 * discipline as `deployment.workflow.ts` (D6) — `../nexus/service.ts` is a
 * pure contract, no `effect` anywhere in this file's import graph.
 */
const ENDPOINT = "flux-deploy"

export async function nexusCallerWorkflow(input: DeploymentInput): Promise<DeploymentResult> {
  const client = wf.createNexusServiceClient({ service: DeployService, endpoint: ENDPOINT })
  return await client.executeOperation("runCanary", input, { scheduleToCloseTimeout: "5 minutes" })
}
