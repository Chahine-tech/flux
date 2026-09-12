import { Context, Effect, Layer } from "effect"
import { DeploymentNotActionable, DeploymentNotFound } from "@flux/contracts"
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
 */
export class TemporalClient extends Context.Service<TemporalClient, {
  readonly start: (request: TriggerDeploymentRequest) => Effect.Effect<string>
  /** Start a multi-service rollout (a parent workflow over one child per service). */
  /** Takes the already-compiled input: the dependency plan is resolved by the handler, not here. */
  readonly startMulti: (input: MultiServiceInput) => Effect.Effect<string>
  readonly status: (workflowId: string) => Effect.Effect<DeploymentState, DeploymentNotFound>
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
  ) => Effect.Effect<ReadonlyArray<DeploymentSummary>>
  /** Ids of the currently-running deployments — the set the poller tracks. */
  readonly listRunningIds: (limit: number) => Effect.Effect<ReadonlyArray<string>>
  /** Closed deployments with their business outcome and duration — projected into the read model. */
  readonly listClosed: (limit: number) => Effect.Effect<ReadonlyArray<ClosedDeployment>>
  readonly approve: (
    workflowId: string
  ) => Effect.Effect<void, DeploymentNotFound | DeploymentNotActionable>
  readonly abort: (workflowId: string) => Effect.Effect<void, DeploymentNotFound>
  /** Create/update the drift-check Schedule for a service; returns its id. */
  readonly ensureDriftSchedule: (
    service: string,
    version: string,
    everyMs: number
  ) => Effect.Effect<string>
  /** Delete the service's drift-check Schedule (idempotent). */
  readonly disableDrift: (service: string) => Effect.Effect<void>
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
      }),

    startMulti: (input) =>
      withClientTraceContext(async () => {
        const workflowId = `multi-${Date.now()}`
        await client.workflow.start("multiServiceDeployment", {
          taskQueue: TASK_QUEUE,
          workflowId,
          args: [input]
        })
        return workflowId
      }),

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
      Effect.promise(async () => {
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
      }),

    listRunningIds: (limit) =>
      Effect.promise(async () => {
        const query = `WorkflowType = '${WORKFLOW_TYPE}' AND ExecutionStatus = 'Running'`
        const ids: Array<string> = []
        for await (const execution of client.workflow.list({ query })) {
          ids.push(execution.workflowId)
          if (ids.length >= limit) break
        }
        return ids
      }),

    listClosed: (limit) =>
      Effect.promise(async () => {
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
const classifyNotFound = (error: unknown, workflowId: string): DeploymentNotFound => {
  if (error instanceof WorkflowNotFoundError) {
    return new DeploymentNotFound({ workflowId })
  }
  throw error
}

/** Update rejected by its validator → 409 (not actionable now); missing → 404. */
const classifyUpdate = (
  error: unknown,
  workflowId: string
): DeploymentNotFound | DeploymentNotActionable => {
  if (error instanceof WorkflowNotFoundError) {
    return new DeploymentNotFound({ workflowId })
  }
  if (error instanceof WorkflowUpdateFailedError) {
    return new DeploymentNotActionable({ workflowId, reason: error.message })
  }
  throw error
}
