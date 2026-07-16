import { Effect, Layer, ManagedRuntime } from "effect"
import { HealthPort, MetricsPort, NotifyPort, RouterPort } from "@flux/application"
import {
  activityInterceptors,
  type AppServices,
  createActivities,
  type DeploymentInput,
  type DeploymentResult,
  decodeHeaderPayload,
  parseTraceparent,
  SEARCH_ATTRIBUTES,
  traceparentClientInterceptor,
  TRACEPARENT_HEADER_KEY,
  withClientTraceContext
} from "@flux/orchestration"
import { Client } from "@temporalio/client"
import { TestWorkflowEnvironment } from "@temporalio/testing"
import { bundleWorkflowCode, Worker } from "@temporalio/worker"
import { afterAll, beforeAll, describe, expect, it } from "vitest"
import { fileURLToPath } from "node:url"

/**
 * D24, end to end: the CLI/control-plane's current Effect span becomes a
 * `traceparent` on the workflow's start headers (client interceptor), the
 * workflow forwards it onto every activity it schedules (the Effect-free
 * workflow-side interceptor, bundled into the VM per D6), and the activity
 * decodes it back into an `ExternalSpan` (worker-side interceptor) — replacing
 * the old runId-derived synthetic root (voie B, N2).
 *
 * Proven the same way D21's codec was: fetch the raw recorded history and read
 * the header off the wire, not by staring at a Jaeger UI.
 */

// temporal.api.enums.v1.IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD
const KEYWORD = 2
const TASK_QUEUE = "flux-tracing"
const workflowsPath = fileURLToPath(import.meta.resolve("@flux/orchestration/workflows"))
const workflowInterceptorModules = [
  fileURLToPath(import.meta.resolve("@flux/orchestration/tracing/workflow-interceptors"))
]

const okPorts = Layer.mergeAll(
  Layer.succeed(HealthPort, { check: () => Effect.void }),
  Layer.succeed(RouterPort, { setTrafficWeight: () => Effect.void, readState: () => Effect.succeed([]) }),
  Layer.succeed(MetricsPort, { query: () => Effect.succeed(0) }),
  Layer.succeed(NotifyPort, { send: () => Effect.void })
) satisfies Layer.Layer<AppServices>

const input: DeploymentInput = {
  service: "api",
  version: "v2",
  previousVersion: "v1",
  steps: [{ percent: 100, monitorMs: 0, requiresApproval: false }],
  rules: [{ name: "errorRate", query: "q", max: 0.5 }],
  pollIntervalMs: 10
}

let env: TestWorkflowEnvironment
let workflowBundle: Awaited<ReturnType<typeof bundleWorkflowCode>>

beforeAll(async () => {
  env = await TestWorkflowEnvironment.createTimeSkipping()
  workflowBundle = await bundleWorkflowCode({ workflowsPath, workflowInterceptorModules })
  // The workflow upserts these on start/finish; the ephemeral server needs them registered.
  await env.connection.operatorService.addSearchAttributes({
    namespace: env.namespace ?? "default",
    searchAttributes: {
      [SEARCH_ATTRIBUTES.service]: KEYWORD,
      [SEARCH_ATTRIBUTES.version]: KEYWORD,
      [SEARCH_ATTRIBUTES.status]: KEYWORD
    }
  }).catch((error: unknown) => {
    if (!/already exist/i.test(String(error))) throw error
  })
}, 60_000)

afterAll(async () => {
  await env?.teardown()
})

describe("D24 traceparent propagation", () => {
  it("carries one traceparent from the client span through to a scheduled activity", async () => {
    const runtime = ManagedRuntime.make(okPorts)
    const client = new Client({
      connection: env.connection,
      namespace: env.namespace ?? "default",
      interceptors: { workflow: [traceparentClientInterceptor] }
    })
    const worker = await Worker.create({
      connection: env.nativeConnection,
      namespace: env.namespace ?? "default",
      taskQueue: TASK_QUEUE,
      workflowBundle,
      activities: createActivities(runtime),
      interceptors: { activity: [activityInterceptors] }
    })

    try {
      const workflowId = `tracing-${Date.now()}`
      await worker.runUntil(async () => {
        const program = withClientTraceContext(() =>
          client.workflow.start("deploymentWorkflow", { taskQueue: TASK_QUEUE, workflowId, args: [input] })
        ).pipe(Effect.withSpan("test.startDeployment"))
        await Effect.runPromise(program)
        await client.workflow.getHandle(workflowId).result()
      })

      const history = await client.workflow.getHandle(workflowId).fetchHistory()
      const events = history.events ?? []

      const startedHeader = events[0]?.workflowExecutionStartedEventAttributes?.header?.fields?.[
        TRACEPARENT_HEADER_KEY
      ]
      const startedValue = decodeHeaderPayload(startedHeader)
      expect(startedValue).toBeDefined()
      expect(parseTraceparent(startedValue!)).toBeDefined()

      // Protobuf oneof quirk found empirically: unset `xxxEventAttributes`
      // fields are not reliably `undefined` on every event — filter by
      // `eventType` instead of attribute presence.
      // `eventType` on the raw protobuf object is numeric, and unset `oneof`
      // attribute fields are not reliably `undefined` — filter by a field only
      // a real ActivityTaskScheduled event actually populates.
      const scheduled = events.find((e) => e.activityTaskScheduledEventAttributes?.activityType?.name !== undefined)
      const scheduledHeader = scheduled?.activityTaskScheduledEventAttributes?.header?.fields?.[
        TRACEPARENT_HEADER_KEY
      ]
      const scheduledValue = decodeHeaderPayload(scheduledHeader)

      // The workflow-side interceptor forwarded the exact same header — one
      // trace, client through activity.
      expect(scheduledValue).toBe(startedValue)
    } finally {
      await runtime.dispose()
    }
  }, 60_000)

  it("falls back to no parent span when the caller never set one", async () => {
    const runtime = ManagedRuntime.make(okPorts)
    const client = new Client({
      connection: env.connection,
      namespace: env.namespace ?? "default",
      interceptors: { workflow: [traceparentClientInterceptor] }
    })
    const worker = await Worker.create({
      connection: env.nativeConnection,
      namespace: env.namespace ?? "default",
      taskQueue: TASK_QUEUE,
      workflowBundle,
      activities: createActivities(runtime),
      interceptors: { activity: [activityInterceptors] }
    })

    try {
      const workflowId = `tracing-no-span-${Date.now()}`
      const result = (await worker.runUntil(
        client.workflow.execute("deploymentWorkflow", { taskQueue: TASK_QUEUE, workflowId, args: [input] })
      )) as DeploymentResult
      expect(result.kind).toBe("Succeeded")

      const history = await client.workflow.getHandle(workflowId).fetchHistory()
      const startedHeader = history.events?.[0]?.workflowExecutionStartedEventAttributes?.header?.fields?.[
        TRACEPARENT_HEADER_KEY
      ]
      expect(decodeHeaderPayload(startedHeader)).toBeUndefined()
    } finally {
      await runtime.dispose()
    }
  }, 60_000)
})
