import { createServer } from "node:http"
import { NativeConnection, Worker } from "@temporalio/worker"
import { activityInterceptors, createActivities, makePayloadCodec, metricsPrometheusText } from "@flux/orchestration"
import type { ManagedRuntime } from "effect"
import type { AppServices } from "@flux/orchestration"
import { makeRuntime } from "./runtime.ts"
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
  server.listen(port, () => console.log(`[flux] metrics on http://localhost:${port}/metrics`))
  return server
}

const main = async (): Promise<void> => {
  const address = process.env.TEMPORAL_ADDRESS ?? "localhost:7233"
  const namespace = process.env.TEMPORAL_NAMESPACE ?? "default"

  const runtime = makeRuntime()
  const metricsServer = startMetricsServer(runtime)
  await ensureSearchAttributes(address, namespace)
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
      console.log(
        `[flux] worker load — wf poller: ${status.workflowPollerState}, act poller: ${status.activityPollerState}, ` +
          `in-flight wf: ${status.numInFlightWorkflowActivations}, act: ${status.numInFlightActivities}, ` +
          `cached wf: ${status.numCachedWorkflows}`
      )
    }, statusIntervalMs)

    console.log(`[flux] worker listening on task queue "${TASK_QUEUE}"`)
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
