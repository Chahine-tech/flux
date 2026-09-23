import { Schema } from "effect"
import { HttpApi, HttpApiEndpoint, HttpApiGroup, HttpApiMiddleware, HttpApiSecurity } from "effect/unstable/httpapi"
import { DeploymentState, DeploymentSummary } from "./deployment.ts"
import { EnableDriftRequest, EnableDriftResponse } from "./drift.ts"
import { StatsResponse } from "./stats.ts"
import { TaskOutcomeRequest, TriggerDeploymentRequest, TriggerDeploymentResponse, TriggerMultiRequest } from "./trigger.ts"

/**
 * The flux HTTP API — one declarative definition shared by both ends.
 *
 * The control plane implements the handlers against this; the CLI derives its
 * typed client from the same value (`HttpApiClient.make(FluxApi)`), so a route,
 * its payload and its result can never drift between server and client. OpenAPI
 * and a Scalar docs page are generated from it for free.
 */

/** Bearer token missing or wrong. */
export class Unauthorized extends Schema.TaggedError<Unauthorized>()(
  "Unauthorized",
  {},
  { httpApiStatus: 401 }
) {}

/**
 * Bearer-token auth on every endpoint, declared in the contract so the OpenAPI
 * docs advertise it and the 401 is typed. The control plane provides the
 * implementation; when it is configured without a token, auth is disabled (dev).
 */
export class Authorization extends HttpApiMiddleware.Service<Authorization>()("flux/Authorization", {
  security: { bearer: HttpApiSecurity.bearer },
  error: Unauthorized
}) {}

/** No deployment with this id is known to Temporal. */
export class DeploymentNotFound extends Schema.TaggedError<DeploymentNotFound>()(
  "DeploymentNotFound",
  { workflowId: Schema.String },
  { httpApiStatus: 404 }
) {}

/**
 * flux could not reach Temporal, or Temporal could not answer.
 *
 * 503 rather than 500: the request is well-formed and the client should retry,
 * nothing is wrong with it. And a typed error rather than an empty result,
 * which is the decision D41 parked and D48 forced. `GET /deployments` returning
 * `[]` during a visibility outage cannot be told apart from "nothing is
 * running", and the CLI prints "no deployments yet" for both, so an outage
 * would read as a quiet, confident lie. Saying nothing is not an option a read
 * endpoint has.
 *
 * `operation` names the call that failed, because "Temporal is unavailable" is
 * not actionable when visibility is degraded but the cluster is otherwise fine,
 * which is the common shape of this outage.
 */
export class TemporalUnavailable extends Schema.TaggedError<TemporalUnavailable>()(
  "TemporalUnavailable",
  { operation: Schema.String, detail: Schema.String },
  { httpApiStatus: 503 }
) {}

/** Build one from whatever the gRPC layer threw. */
export const temporalUnavailable = (operation: string, error: unknown): TemporalUnavailable =>
  new TemporalUnavailable({ operation, detail: error instanceof Error ? error.message : String(error) })

/** The action is invalid in the deployment's current state (e.g. approving one that isn't awaiting approval). */
export class DeploymentNotActionable extends Schema.TaggedError<DeploymentNotActionable>()(
  "DeploymentNotActionable",
  { workflowId: Schema.String, reason: Schema.String },
  { httpApiStatus: 409 }
) {}

/** The global concurrent-deployment budget is full (admission control). */
export class DeploymentBudgetExhausted extends Schema.TaggedError<DeploymentBudgetExhausted>()(
  "DeploymentBudgetExhausted",
  { service: Schema.String, limit: Schema.Finite },
  { httpApiStatus: 429 }
) {}

/** The service already has a deployment in flight — one at a time (admission control). */
export class ServiceAlreadyDeploying extends Schema.TaggedError<ServiceAlreadyDeploying>()(
  "ServiceAlreadyDeploying",
  { service: Schema.String },
  { httpApiStatus: 409 }
) {}

/** The deploy was triggered outside its allowed window; retry after `nextAllowed`. */
export class OutsideDeploymentWindow extends Schema.TaggedError<OutsideDeploymentWindow>()(
  "OutsideDeploymentWindow",
  { service: Schema.String, window: Schema.String, nextAllowed: Schema.String },
  { httpApiStatus: 422 }
) {}

/**
 * The declared dependencies do not form a usable rollout: a cycle, a
 * dependency on a service absent from the rollout, or a service depending on
 * itself. `detail` is human-readable and names the services involved, because
 * "invalid configuration" is useless when you have twenty of them.
 */
export class InvalidRolloutPlan extends Schema.TaggedError<InvalidRolloutPlan>()(
  "InvalidRolloutPlan",
  { reason: Schema.Literals(["Cycle", "UnknownDependency", "SelfDependency"]), detail: Schema.String },
  { httpApiStatus: 422 }
) {}

const WorkflowIdParam = { workflowId: Schema.String }

const deployments = HttpApiGroup.make("deployments")
  .add(
    HttpApiEndpoint.post("trigger", "/deployments", {
      payload: TriggerDeploymentRequest,
      success: TriggerDeploymentResponse,
      error: [DeploymentBudgetExhausted, ServiceAlreadyDeploying, OutsideDeploymentWindow, TemporalUnavailable]
    })
  )
  .add(
    HttpApiEndpoint.post("triggerMulti", "/deployments/multi", {
      payload: TriggerMultiRequest,
      success: TriggerDeploymentResponse,
      // A rollout is admitted as a unit, so it can be refused for the same two
      // reasons a single deployment can.
      error: [InvalidRolloutPlan, DeploymentBudgetExhausted, ServiceAlreadyDeploying, TemporalUnavailable]
    })
  )
  .add(
    HttpApiEndpoint.post("enableDrift", "/drift", {
      payload: EnableDriftRequest,
      success: EnableDriftResponse,
      error: TemporalUnavailable
    })
  )
  .add(
    // Idempotent — disabling drift for a service that has none is a 204 too.
    HttpApiEndpoint.delete("disableDrift", "/drift/:service", {
      params: { service: Schema.String },
      error: TemporalUnavailable
    })
  )
  .add(
    HttpApiEndpoint.get("list", "/deployments", {
      query: {
        service: Schema.optional(Schema.String),
        limit: Schema.optional(Schema.FiniteFromString)
      },
      success: Schema.Array(DeploymentSummary),
      error: TemporalUnavailable
    })
  )
  .add(
    HttpApiEndpoint.get("status", "/deployments/:workflowId", {
      params: WorkflowIdParam,
      success: DeploymentState,
      error: [DeploymentNotFound, TemporalUnavailable]
    })
  )
  .add(
    HttpApiEndpoint.post("approve", "/deployments/:workflowId/approve", {
      params: WorkflowIdParam,
      error: [DeploymentNotFound, DeploymentNotActionable, TemporalUnavailable]
    })
  )
  .add(
    HttpApiEndpoint.post("abort", "/deployments/:workflowId/abort", {
      params: WorkflowIdParam,
      error: [DeploymentNotFound, TemporalUnavailable]
    })
  )
  .add(
    /**
     * Report one unit of work's outcome to a running deployment.
     *
     * For verdicts that only exist later: the pull request merged, the suite
     * went green, a reviewer accepted the work. The caller is whatever learned
     * it, usually minutes or hours after flux routed the work, and it lands as
     * a Temporal signal so it is accepted whether or not a worker is up.
     *
     * Fire and forget by design. There is no acknowledgement of *counting*
     * beyond the request succeeding, because the sender has nothing useful to
     * do with the tally and waiting for one would make an outside system's
     * write depend on a worker being alive.
     */
    HttpApiEndpoint.post("recordTaskOutcome", "/deployments/:workflowId/outcomes", {
      params: WorkflowIdParam,
      payload: TaskOutcomeRequest,
      error: [DeploymentNotFound, TemporalUnavailable]
    })
  )

const stats = HttpApiGroup.make("stats").add(
  HttpApiEndpoint.get("stats", "/stats", {
    success: StatsResponse
  })
)

export const FluxApi = HttpApi.make("flux").add(deployments).add(stats).middleware(Authorization)
