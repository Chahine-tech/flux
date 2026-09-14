import { createServer } from "node:http"
import { NativeConnection, Runtime, Worker } from "@temporalio/worker"
import { activityInterceptors, createActivities, makePayloadCodec, metricsPrometheusText } from "@flux/orchestration"
import { Effect, type ManagedRuntime } from "effect"
import type { AppServices } from "@flux/orchestration"
import { makeRuntime } from "./runtime.ts"
import { effectLogger } from "./temporal-logger.ts"
import { ensureSearchAttributes } from "./search-attributes.ts"
import { pollerBehaviors, tuner, versioningOptions, workflowSource } from "./worker-config.ts"

/**
 * flux worker — Temporal process.
 *
 * Builds the ManagedRuntime once, wires the activities around it, and runs a
 * Temporal Worker. The runtime is disposed on shutdown. Workflows are bundled
 * by Temporal from the `@flux/orchestration/workflows` entry point.
 */
const TASK_QUEUE = "flux-deployments"

/** Serve the Effect metric registry as Prometheus text on `/metrics`. */
const startMetricsServer = (runtime: ManagedRuntime.ManagedRuntime<AppServices, never>) => {
  const port = Number(process.env.METRICS_PORT ?? 9464)
  const server = createServer((req, res) => {
    if (req.url !== "/metrics") {
      res.writeHead(404).end()
      return
    }
    runtime.runPromise(metricsPrometheusText).then(
      (text) => res.writeHead(200, { "content-type": "text/plain; version=0.0.4" }).end(text),
      () => res.writeHead(500).end()
    )
  })
  server.listen(port, () => runtime.runFork(Effect.annotateLogs(Effect.logInfo("metrics served"), { port })))
  return server
}

const main = async (): Promise<void> => {
  const address = process.env.TEMPORAL_ADDRESS ?? "localhost:7233"
  const namespace = process.env.TEMPORAL_NAMESPACE ?? "default"

  const runtime = makeRuntime()
  // Before anything else touches the SDK: `Runtime.install` fails once a
  // Runtime exists, and `ensureSearchAttributes` below opens a connection.
  // From here the SDK's own logs and every workflow's `log.*` arrive in
  // Effect's logger, correlated with the activity logs of the same deployment
  // instead of going out to stderr on their own.
  Runtime.install({ logger: effectLogger(runtime) })
  const metricsServer = startMetricsServer(runtime)
  await ensureSearchAttributes((message) => runtime.runFork(Effect.logInfo(message)), address, namespace)
  const connection = await NativeConnection.connect({ address })

  try {
    const workerDeploymentOptions = versioningOptions()
    const workflows = workflowSource()
    const worker = await Worker.create({
      connection,
      namespace,
      taskQueue: TASK_QUEUE,
      ...workflows.source,
      activities: createActivities(runtime),
      // Large payloads are gzip-compressed on the wire and in history.
      // The codec runs here on the main thread, never inside the workflow VM.
      dataConverter: { payloadCodecs: [makePayloadCodec()] },
      // Both hops of the single trace in one object. The workflow-side modules
      // come from `workflowSource()` and are empty on the prebuilt-bundle
      // route; splitting this into two `interceptors` keys would let the
      // second silently overwrite the first.
      interceptors: { activity: [activityInterceptors], workflowModules: workflows.workflowModules },
      tuner,
      // Poller autoscaling: the number of open polls tracks the queue
      // backlog between the configured min/max, so idle workers stay cheap and a
      // burst of deployments scales up — no Kubernetes, no fixed poll count to guess.
      ...pollerBehaviors(),
      ...(workerDeploymentOptions ? { workerDeploymentOptions } : {})
    })

    // Log the worker's live load periodically so the autoscaling is observable
    // (poller state + in-flight work).
    const statusIntervalMs = Number(process.env.WORKER_STATUS_INTERVAL_MS ?? 30_000)
    const statusInterval = setInterval(() => {
      const status = worker.getStatus()
      // Through the logger, not `console.log`. A raw write bypasses every
      // decision the logger makes: it is not JSON where something is parsing,
      // it carries no annotations, and the span tree cannot place it, so it
      // lands in the middle of a drawing it is not part of.
      runtime.runFork(
        Effect.annotateLogs(Effect.logInfo("worker load"), {
          workflowPoller: status.workflowPollerState,
          activityPoller: status.activityPollerState,
          inFlightWorkflows: status.numInFlightWorkflowActivations,
          inFlightActivities: status.numInFlightActivities,
          cachedWorkflows: status.numCachedWorkflows
        })
      )
    }, statusIntervalMs)

    runtime.runFork(Effect.annotateLogs(Effect.logInfo("worker listening"), { taskQueue: TASK_QUEUE }))
    try {
      await worker.run()
    } finally {
      clearInterval(statusInterval)
    }
  } finally {
    metricsServer.close()
    await runtime.dispose()
    await connection.close()
  }
}

main().catch((error: unknown) => {
  console.error(error)
  process.exit(1)
})
