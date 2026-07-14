import { createServer } from "node:http"
import { fileURLToPath } from "node:url"
import { NativeConnection, Worker, type WorkerTuner } from "@temporalio/worker"
import { createActivities, metricsPrometheusText } from "@flux/orchestration"
import type { ManagedRuntime } from "effect"
import type { AppServices } from "@flux/orchestration"
import { makeRuntime } from "./runtime.ts"
import { ensureSearchAttributes } from "./search-attributes.ts"

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

/**
 * Deployment-based Worker Versioning (N4/D15): when a build id is provided,
 * this worker joins a named deployment and pins in-flight workflows to their
 * version, so a rolling upgrade (v1 → v2) never breaks a canary mid-flight —
 * new deployments start on v2, ones already running finish on v1. Left off in
 * dev/tests (no build id), where a versioning-capable server isn't required.
 */
const versioningOptions = () => {
  const buildId = process.env.FLUX_WORKER_BUILD_ID
  if (buildId === undefined) {
    return undefined
  }
  return {
    version: { deploymentName: process.env.FLUX_WORKER_DEPLOYMENT ?? "flux-worker", buildId },
    useWorkerVersioning: true as const,
    defaultVersioningBehavior: "PINNED" as const
  }
}

/**
 * Resource-based slot tuning (N4/D18). flux's slot profiles genuinely differ:
 * monitoring is a small number of long-lived, heartbeating activities that each
 * hold a slot for a whole window, so activity slots are capped by *resource
 * pressure* rather than a fixed count that could over-commit memory under a
 * burst of deployments. Health checks are fast local activities, given a wider
 * burst with no ramp throttle.
 */
const tuner: WorkerTuner = {
  tunerOptions: {
    targetMemoryUsage: Number(process.env.WORKER_TARGET_MEMORY ?? 0.8),
    targetCpuUsage: Number(process.env.WORKER_TARGET_CPU ?? 0.9)
  },
  activityTaskSlotOptions: { minimumSlots: 1, maximumSlots: 200, rampThrottle: "50ms" },
  localActivityTaskSlotOptions: { minimumSlots: 2, maximumSlots: 500, rampThrottle: "0ms" }
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
    const worker = await Worker.create({
      connection,
      namespace,
      taskQueue: TASK_QUEUE,
      workflowsPath: fileURLToPath(import.meta.resolve("@flux/orchestration/workflows")),
      activities: createActivities(runtime),
      tuner,
      ...(workerDeploymentOptions ? { workerDeploymentOptions } : {})
    })

    console.log(`[flux] worker listening on task queue "${TASK_QUEUE}"`)
    await worker.run()
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
