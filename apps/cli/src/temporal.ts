import { Client, Connection } from "@temporalio/client"
import type { DeploymentInput } from "@flux/orchestration"

/**
 * Thin Temporal client helpers for the CLI's `direct` mode (embedded client).
 * Promise-based; the commands wrap these in `Effect.promise`. A proper scoped
 * `TemporalClient` Layer arrives with the control plane (N3).
 */

const TASK_QUEUE = "flux-deployments"
const WORKFLOW_TYPE = "deploymentWorkflow"

const address = (): string => process.env.TEMPORAL_ADDRESS ?? "localhost:7233"
const namespace = (): string => process.env.TEMPORAL_NAMESPACE ?? "default"

const withClient = async <A>(use: (client: Client) => Promise<A>): Promise<A> => {
  const connection = await Connection.connect({ address: address() })
  try {
    return await use(new Client({ connection, namespace: namespace() }))
  } finally {
    await connection.close()
  }
}

export const startDeployment = (input: DeploymentInput): Promise<string> =>
  withClient(async (client) => {
    const workflowId = `dep-${input.service}-${Date.now()}`
    await client.workflow.start(WORKFLOW_TYPE, {
      taskQueue: TASK_QUEUE,
      workflowId,
      args: [input]
    })
    return workflowId
  })

export const signalDeployment = (
  workflowId: string,
  signal: "approve" | "abort"
): Promise<void> => withClient((client) => client.workflow.getHandle(workflowId).signal(signal))

export const describeDeployment = (workflowId: string): Promise<string> =>
  withClient(async (client) => {
    const description = await client.workflow.getHandle(workflowId).describe()
    return description.status.name
  })
