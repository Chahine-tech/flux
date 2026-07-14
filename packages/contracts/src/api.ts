import { Schema } from "effect"
import { HttpApi, HttpApiEndpoint, HttpApiGroup } from "effect/unstable/httpapi"
import { DeploymentState, DeploymentSummary } from "./deployment.ts"
import { EnableDriftRequest, EnableDriftResponse } from "./drift.ts"
import { StatsResponse } from "./stats.ts"
import { TriggerDeploymentRequest, TriggerDeploymentResponse, TriggerMultiRequest } from "./trigger.ts"

/**
 * The flux HTTP API — one declarative definition shared by both ends (N3).
 *
 * The control plane implements the handlers against this; the CLI derives its
 * typed client from the same value (`HttpApiClient.make(FluxApi)`), so a route,
 * its payload and its result can never drift between server and client. OpenAPI
 * and a Scalar docs page are generated from it for free.
 */

/** No deployment with this id is known to Temporal. */
export class DeploymentNotFound extends Schema.TaggedErrorClass<DeploymentNotFound>()(
  "DeploymentNotFound",
  { workflowId: Schema.String },
  { httpApiStatus: 404 }
) {}

/** The action is invalid in the deployment's current state (e.g. approving one that isn't awaiting approval). */
export class DeploymentNotActionable extends Schema.TaggedErrorClass<DeploymentNotActionable>()(
  "DeploymentNotActionable",
  { workflowId: Schema.String, reason: Schema.String },
  { httpApiStatus: 409 }
) {}

/** The global concurrent-deployment budget is full (admission control, N4/D14). */
export class DeploymentBudgetExhausted extends Schema.TaggedErrorClass<DeploymentBudgetExhausted>()(
  "DeploymentBudgetExhausted",
  { service: Schema.String, limit: Schema.Finite },
  { httpApiStatus: 429 }
) {}

/** The service already has a deployment in flight — one at a time (admission control, N4/D14). */
export class ServiceAlreadyDeploying extends Schema.TaggedErrorClass<ServiceAlreadyDeploying>()(
  "ServiceAlreadyDeploying",
  { service: Schema.String },
  { httpApiStatus: 409 }
) {}

const WorkflowIdParam = { workflowId: Schema.String }

const deployments = HttpApiGroup.make("deployments")
  .add(
    HttpApiEndpoint.post("trigger", "/deployments", {
      payload: TriggerDeploymentRequest,
      success: TriggerDeploymentResponse,
      error: [DeploymentBudgetExhausted, ServiceAlreadyDeploying]
    })
  )
  .add(
    HttpApiEndpoint.post("triggerMulti", "/deployments/multi", {
      payload: TriggerMultiRequest,
      success: TriggerDeploymentResponse
    })
  )
  .add(
    HttpApiEndpoint.post("enableDrift", "/drift", {
      payload: EnableDriftRequest,
      success: EnableDriftResponse
    })
  )
  .add(
    HttpApiEndpoint.get("list", "/deployments", {
      query: {
        service: Schema.optional(Schema.String),
        limit: Schema.optional(Schema.FiniteFromString)
      },
      success: Schema.Array(DeploymentSummary)
    })
  )
  .add(
    HttpApiEndpoint.get("status", "/deployments/:workflowId", {
      params: WorkflowIdParam,
      success: DeploymentState,
      error: DeploymentNotFound
    })
  )
  .add(
    HttpApiEndpoint.post("approve", "/deployments/:workflowId/approve", {
      params: WorkflowIdParam,
      error: [DeploymentNotFound, DeploymentNotActionable]
    })
  )
  .add(
    HttpApiEndpoint.post("abort", "/deployments/:workflowId/abort", {
      params: WorkflowIdParam,
      error: DeploymentNotFound
    })
  )

const stats = HttpApiGroup.make("stats").add(
  HttpApiEndpoint.get("stats", "/stats", {
    success: StatsResponse
  })
)

export const FluxApi = HttpApi.make("flux").add(deployments).add(stats)
