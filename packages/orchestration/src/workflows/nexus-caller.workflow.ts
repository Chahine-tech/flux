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
  // The operation spans the whole backing canary, and a canary's monitoring
  // windows are long by design (a 15-minute canary is the README's opening
  // example) — a short schedule-to-close would time the operation out
  // mid-monitor. Sized for the longest realistic rollout, not for the test.
  return await client.executeOperation("runCanary", input, { scheduleToCloseTimeout: "2 hours" })
}
