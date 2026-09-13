import { Context, Effect, Layer } from "effect"
import { DeploymentNotActionable, DeploymentNotFound, temporalUnavailable, TemporalUnavailable } from "@flux/contracts"
import type { DeploymentState, DeploymentSummary, TriggerDeploymentRequest, TriggerMultiRequest } from "@flux/contracts"
import type { DeploymentInput, MultiServiceInput } from "@flux/orchestration"
import { makePayloadCodec, SEARCH_ATTRIBUTES, traceparentClientInterceptor, withClientTraceContext } from "@flux/orchestration"
import {
  Client,
  Connection,
  WorkflowNotFoundError,
  WorkflowUpdateFailedError
} from "@temporalio/client"
import { deleteDriftSchedule, ensureDriftSchedule as ensureDriftScheduleImpl } from "./schedules.ts"

/**
 * Port to Temporal for the control plane — the single place the HTTP handlers
 * and the pollers reach the cluster. Temporal's client is Promise-based, so
 * every method is wrapped in an Effect and its Promise rejections are classified
 * into the API's typed errors, keeping the callers pure.
 *
 * **Every method that talks to the cluster can fail with `TemporalUnavailable`,
 * and that used to be a lie by omission.** The read methods wrapped gRPC in
 * `Effect.promise`, whose rejections become *defects*, and `status` used a
 * `catch` that re-threw anything it could not classify, which is a defect too.
 * Neither showed up in a signature, so callers wrote guards against the errors
 * the types admitted and were taken down by the ones they did not (D41's fourth
 * finding, and the mechanism behind D48's admission leak). The types now say
 * what can actually happen, which is the point of having them.
 */
export class TemporalClient extends Context.Service<TemporalClient, {
  readonly start: (request: TriggerDeploymentRequest) => Effect.Effect<string, TemporalUnavailable>
  /** Start a multi-service rollout (a parent workflow over one child per service). */
  /** Takes the already-compiled input: the dependency plan is resolved by the handler, not here. */
  readonly startMulti: (input: MultiServiceInput) => Effect.Effect<string, TemporalUnavailable>
  readonly status: (workflowId: string) => Effect.Effect<DeploymentState, DeploymentNotFound | TemporalUnavailable>
  /**
   * Whether a workflow is still open, for reconciling admission slots.
   *
   * `describe` rather than a visibility query, because visibility is only
   * eventually consistent and this answer decides whether to free a slot: a
   * lagging index would report a live rollout as gone and let a second
   * deployment of the same service in. `describe` reads the mutable state
   * directly, so "missing" means it really does not exist rather than "not
   * indexed yet".
   *
   * Total on purpose. A caller reconciling slots has nothing useful to do with
   * a transient failure except leave the slot alone until the next tick, which
   * is what `Unknown` says.
   */
  readonly execution: (workflowId: string) => Effect.Effect<"open" | "closed" | "missing" | "unknown">
  /**
   * Whether the cluster is reachable *and* the namespace flux works in is
   * registered — a readiness signal, not a liveness one.
   *
   * `describeNamespace` rather than `getSystemInfo` on purpose: a cluster that
   * is up but whose namespace is missing or deregistered would pass the cheaper
   * call and then fail every real operation.
   */
  readonly reachable: Effect.Effect<boolean>
  readonly list: (
    service: string | undefined,
    limit: number
  ) => Effect.Effect<ReadonlyArray<DeploymentSummary>, TemporalUnavailable>
  /** Ids of the currently-running deployments — the set the poller tracks. */
  readonly listRunningIds: (limit: number) => Effect.Effect<ReadonlyArray<string>, TemporalUnavailable>
  /** Closed deployments with their business outcome and duration — projected into the read model. */
  readonly listClosed: (limit: number) => Effect.Effect<ReadonlyArray<ClosedDeployment>, TemporalUnavailable>
  readonly approve: (
    workflowId: string
  ) => Effect.Effect<void, DeploymentNotFound | DeploymentNotActionable | TemporalUnavailable>
  readonly abort: (workflowId: string) => Effect.Effect<void, DeploymentNotFound | TemporalUnavailable>
  /** Create/update the drift-check Schedule for a service; returns its id. */
  readonly ensureDriftSchedule: (
    service: string,
    version: string,
    everyMs: number
  ) => Effect.Effect<string, TemporalUnavailable>
  /** Delete the service's drift-check Schedule (idempotent). */
  readonly disableDrift: (service: string) => Effect.Effect<void, TemporalUnavailable>
}>()("TemporalClient") {}

const TASK_QUEUE = "flux-deployments"
const WORKFLOW_TYPE = "deploymentWorkflow"

/** A finished deployment, as projected into the CQRS read model. */
export interface ClosedDeployment {
  readonly workflowId: string
  readonly service: string
  /** Business outcome from the `FluxStatus` search attribute (Succeeded/RolledBack/Aborted/Failed/RollbackFailed). */
  readonly status: string
  readonly durationMs: number
}

// temporal.api.enums.v1.NamespaceState.NAMESPACE_STATE_REGISTERED
const NAMESPACE_STATE_REGISTERED = 1

const firstString = (value: unknown): string | undefined =>
  Array.isArray(value) && typeof value[0] === "string" ? value[0] : undefined

/**
 * Build the port around an existing Temporal `Client`. Kept separate from the
 * connection lifecycle so an integration test can drive the exact client code
 * production uses against a test server's client.
 */
export const make = (client: Client): typeof TemporalClient.Service => {
  const handle = (workflowId: string) => client.workflow.getHandle(workflowId)

  return {
    // The current span (the HTTP request's) becomes the trace root the
    // whole deployment — CLI/control-plane through every activity — shares.
    start: (request) =>
      withClientTraceContext(async () => {
        const workflowId = `dep-${request.service}-${Date.now()}`
        await client.workflow.start(WORKFLOW_TYPE, {
          taskQueue: TASK_QUEUE,
          workflowId,
          // Fair share of the task queue, keyed by service. Activities and
          // child workflows inherit this, so it is set once here rather than
          // on every proxy. D35's rollback priority overrides `priorityKey`
          // only and keeps this key, which is the composition the SDK
          // documents: priority decides who goes first, fairness decides how
          // the rest of the queue is shared out.
          priority: { fairnessKey: request.service },
          // The request is structurally the workflow's Effect-free input.
          args: [request as DeploymentInput]
        })
        return workflowId
      }, (error) => unavailable("start", error)),

    startMulti: (input) =>
      withClientTraceContext(async () => {
        const workflowId = `multi-${Date.now()}`
        await client.workflow.start("multiServiceDeployment", {
          taskQueue: TASK_QUEUE,
          workflowId,
          args: [input]
        })
        return workflowId
      }, (error) => unavailable("startMulti", error)),

    status: (workflowId) =>
      Effect.tryPromise({
        try: () => handle(workflowId).query<DeploymentState>("status"),
        catch: (error) => classifyNotFound(error, workflowId)
      }),

    execution: (workflowId) =>
      Effect.tryPromise({
        try: async () => {
          const description = await handle(workflowId).describe()
          // `status.name` is the string form; RUNNING and the continued-as-new
          // states are the ones that still hold their slot.
          return description.status.name === "RUNNING" ? "open" as const : "closed" as const
        },
        catch: (error) => (error instanceof WorkflowNotFoundError ? "missing" as const : "unknown" as const)
      }).pipe(Effect.catch(Effect.succeed)),

    reachable: Effect.tryPromise(async () => {
      const { namespaceInfo } = await client.connection.workflowService.describeNamespace({
        namespace: client.options.namespace
      })
      // REGISTERED is the only state that can serve work; DEPRECATED and
      // DELETED are reachable but useless to a deployment. The wire value is
      // the numeric enum — `JSON.stringify` renders it as its name, which is
      // what made an early version of this compare against a string and report
      // "not ready" against a perfectly healthy cluster.
      return namespaceInfo?.state === NAMESPACE_STATE_REGISTERED
    }).pipe(Effect.catchCause(() => Effect.succeed(false))),

    list: (service, limit) =>
      Effect.tryPromise({
        try: async () => {
        const filter = service === undefined || service === ""
          ? ""
          : ` AND ${SEARCH_ATTRIBUTES.service} = '${service}'`
        const query = `WorkflowType = '${WORKFLOW_TYPE}'${filter}`
        const summaries: Array<DeploymentSummary> = []
        for await (const execution of client.workflow.list({ query })) {
          summaries.push({
            workflowId: execution.workflowId,
            status: execution.status.name,
            startTime: execution.startTime.toISOString()
          })
          if (summaries.length >= limit) break
        }
        return summaries
        },
        catch: (error) => unavailable("list", error)
      }),

    listRunningIds: (limit) =>
      Effect.tryPromise({
        try: async () => {
        const query = `WorkflowType = '${WORKFLOW_TYPE}' AND ExecutionStatus = 'Running'`
        const ids: Array<string> = []
        for await (const execution of client.workflow.list({ query })) {
          ids.push(execution.workflowId)
          if (ids.length >= limit) break
        }
        return ids
        },
        catch: (error) => unavailable("listRunningIds", error)
      }),

    listClosed: (limit) =>
      Effect.tryPromise({
        try: async () => {
        // flux workflows always complete normally (they return a result even on
        // rollback/failure); the business outcome lives in FluxStatus.
        const query = `WorkflowType = '${WORKFLOW_TYPE}' AND ExecutionStatus = 'Completed'`
        const closed: Array<ClosedDeployment> = []
        for await (const execution of client.workflow.list({ query })) {
          const attributes = execution.searchAttributes as Record<string, ReadonlyArray<unknown> | undefined>
          const status = firstString(attributes["FluxStatus"])
          if (status === undefined || execution.closeTime === undefined) continue
          closed.push({
            workflowId: execution.workflowId,
            service: firstString(attributes["FluxService"]) ?? execution.workflowId,
            status,
            durationMs: execution.closeTime.getTime() - execution.startTime.getTime()
          })
          if (closed.length >= limit) break
        }
        return closed
        },
        catch: (error) => unavailable("listClosed", error)
      }),

    approve: (workflowId) =>
      Effect.tryPromise({
        try: () => handle(workflowId).executeUpdate("approve"),
        catch: (error) => classifyUpdate(error, workflowId)
      }),

    abort: (workflowId) =>
      Effect.tryPromise({
        try: () => handle(workflowId).executeUpdate("abort"),
        catch: (error) => classifyNotFound(error, workflowId)
      }),

    ensureDriftSchedule: (service, version, everyMs) =>
      ensureDriftScheduleImpl(client, {
        desired: { service, desired: [{ version, weight: 100 }], reconcile: true },
        everyMs
      }),

    disableDrift: (service) => deleteDriftSchedule(client, service)
  }
}

export interface TemporalClientConfig {
  readonly address: string
  readonly namespace: string
}

/**
 * Production layer: open a Temporal connection (scoped — closed on shutdown) and
 * build the port around it.
 */
export const layer = (config: TemporalClientConfig): Layer.Layer<TemporalClient> =>
  Layer.effect(
    TemporalClient,
    Effect.gen(function*() {
      const connection = yield* Effect.acquireRelease(
        Effect.promise(() => Connection.connect({ address: config.address })),
        (conn) => Effect.promise(() => conn.close())
      )
      return make(
        new Client({
          connection,
          namespace: config.namespace,
          // Symmetric with the worker: large payloads travel gzipped.
          dataConverter: { payloadCodecs: [makePayloadCodec()] },
          interceptors: { workflow: [traceparentClientInterceptor] }
        })
      )
    })
  )

/** Layer over an already-built client (integration tests, embedding). */
export const layerFromClient = (client: Client): Layer.Layer<TemporalClient> =>
  Layer.succeed(TemporalClient, make(client))

/** Missing workflow → 404; anything else is an unexpected defect (Effect dies). */
/**
 * Anything that is not a missing workflow is the cluster being unable to
 * answer. This used to `throw` instead, which turned a Temporal outage into a
 * defect: invisible in the signature, and straight through every caller's
 * guard. Returning it keeps it in the error channel where callers can see it.
 */
const classifyNotFound = (error: unknown, workflowId: string): DeploymentNotFound | TemporalUnavailable =>
  error instanceof WorkflowNotFoundError
    ? new DeploymentNotFound({ workflowId })
    : unavailable("status", error)

/** Every gRPC failure that is not a business outcome looks the same to a caller. */
const unavailable = temporalUnavailable

/** Update rejected by its validator → 409 (not actionable now); missing → 404. */
const classifyUpdate = (
  error: unknown,
  workflowId: string
): DeploymentNotFound | DeploymentNotActionable | TemporalUnavailable => {
  if (error instanceof WorkflowNotFoundError) {
    return new DeploymentNotFound({ workflowId })
  }
  if (error instanceof WorkflowUpdateFailedError) {
    return new DeploymentNotActionable({ workflowId, reason: error.message })
  }
  return unavailable("update", error)
}
