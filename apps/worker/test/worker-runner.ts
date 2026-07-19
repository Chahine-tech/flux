import { fileURLToPath } from "node:url"
import { NativeConnection, Worker } from "@temporalio/worker"
import { activities } from "./worker-doubles.ts"

/**
 * Child worker for the D27 worker-SIGKILL proof — the Temporal-side twin of
 * `packages/comparison/test/cluster-runner.ts`. Runs the real workflow bundle
 * with the shared logging doubles; the test kills this process mid-monitor
 * and hands the task queue to a fresh worker, which must complete the canary
 * without re-executing the activities this one already finished.
 *
 * argv: <task-queue> <shift-log-path>
 */
const [taskQueue, shiftLog] = process.argv.slice(2)
if (!taskQueue || !shiftLog) {
  console.error("usage: worker-runner.ts <task-queue> <shift-log-path>")
  process.exit(2)
}

const address = process.env.TEMPORAL_ADDRESS ?? "localhost:7233"
const namespace = process.env.TEMPORAL_NAMESPACE ?? "default"
const workflowsPath = fileURLToPath(import.meta.resolve("@flux/orchestration/workflows"))

const main = async (): Promise<void> => {
  const connection = await NativeConnection.connect({ address })
  const worker = await Worker.create({
    connection,
    namespace,
    taskQueue,
    workflowsPath,
    activities: activities(shiftLog)
  })
  console.log("WORKER_READY")
  await worker.run()
}

main().catch((error: unknown) => {
  console.error(error)
  process.exit(1)
})
