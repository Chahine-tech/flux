import { fileURLToPath } from "node:url"
import { NativeConnection, Worker } from "@temporalio/worker"
import { createActivities } from "@flux/orchestration"
import { makeRuntime } from "./runtime.ts"

/**
 * flux worker — Temporal process (ARCHITECTURE.md D7).
 *
 * Builds the ManagedRuntime once, wires the activities around it, and runs a
 * Temporal Worker. The runtime is disposed on shutdown. Workflows are bundled
 * by Temporal from the `@flux/orchestration/workflows` entry point.
 */
const TASK_QUEUE = "flux-deployments"

const main = async (): Promise<void> => {
  const runtime = makeRuntime()
  const connection = await NativeConnection.connect({
    address: process.env.TEMPORAL_ADDRESS ?? "localhost:7233"
  })

  try {
    const worker = await Worker.create({
      connection,
      namespace: process.env.TEMPORAL_NAMESPACE ?? "default",
      taskQueue: TASK_QUEUE,
      workflowsPath: fileURLToPath(import.meta.resolve("@flux/orchestration/workflows")),
      activities: createActivities(runtime)
    })

    console.log(`[flux] worker listening on task queue "${TASK_QUEUE}"`)
    await worker.run()
  } finally {
    await runtime.dispose()
    await connection.close()
  }
}

main().catch((error: unknown) => {
  console.error(error)
  process.exit(1)
})
