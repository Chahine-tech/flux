import { Client, Connection } from "@temporalio/client"
import { type DeploymentState, makePayloadCodec, SEARCH_ATTRIBUTES } from "@flux/orchestration"

export interface DeploymentSummary {
  readonly workflowId: string
  readonly status: string
  readonly startTime: string
}

/**
 * Thin Temporal client helpers for the CLI's read/control commands (status,
 * history, approve, abort). Writes (`deploy`, `deploy-multi`) go through the
 * control plane instead, so they pass admission control.
 */

const WORKFLOW_TYPE = "deploymentWorkflow"

const address = (): string => process.env.TEMPORAL_ADDRESS ?? "localhost:7233"
const namespace = (): string => process.env.TEMPORAL_NAMESPACE ?? "default"

const withClient = async <A>(use: (client: Client) => Promise<A>): Promise<A> => {
  const connection = await Connection.connect({ address: address() })
  try {
    return await use(
      new Client({
        connection,
        namespace: namespace(),
        // Symmetric with the worker (D21): history payloads may be gzipped.
        dataConverter: { payloadCodecs: [makePayloadCodec()] }
      })
    )
  } finally {
    await connection.close()
  }
}

/** approve/abort are validated Updates — a rejected update surfaces as an error. */
export const updateDeployment = (
  workflowId: string,
  update: "approve" | "abort"
): Promise<void> => withClient((client) => client.workflow.getHandle(workflowId).executeUpdate(update))

/** Read the live canary state via the workflow's `status` query. */
export const queryStatus = (workflowId: string): Promise<DeploymentState> =>
  withClient((client) => client.workflow.getHandle(workflowId).query<DeploymentState>("status"))

/** List recent deployments via advanced-visibility search attributes. */
export const listDeployments = (service: string, limit: number): Promise<Array<DeploymentSummary>> =>
  withClient(async (client) => {
    const serviceFilter = service === "" ? "" : ` AND ${SEARCH_ATTRIBUTES.service} = '${service}'`
    const query = `WorkflowType = 'deploymentWorkflow'${serviceFilter}`
    const summaries: Array<DeploymentSummary> = []
    for await (const execution of client.workflow.list({ query })) {
      summaries.push({
        workflowId: execution.workflowId,
        status: execution.status.name,
        startTime: execution.startTime.toISOString()
      })
      if (summaries.length >= limit) {
        break
      }
    }
    return summaries
  })
