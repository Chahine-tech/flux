import { Console, Effect, Layer, ManagedRuntime, Ref } from "effect"
import { Client, Connection } from "@temporalio/client"
import { NativeConnection, Worker } from "@temporalio/worker"
import { fileURLToPath } from "node:url"
import { HealthPort, MetricsPort, NotifyPort, RouterPort } from "@flux/application"
import { createActivities, type DeploymentInput, type DeploymentResult } from "@flux/orchestration"

/**
 * Self-contained e2e demo — runs the real canary workflow against a real
 * Temporal server and shows an automatic rollback.
 *
 * The 4 ports are backed by in-memory demo Layers so the flow is deterministic
 * and readable: metrics are healthy at 10% then breach at 50%, forcing the
 * workflow to roll back. The orchestration (workflow + activities + Temporal
 * durability) is the real thing — that is what this validates.
 *
 * Prereq: `docker compose up -d`. Run with `pnpm --filter @flux/worker demo`.
 */

// Metrics: healthy on the first monitored step, breaching afterwards.
const DemoMetrics = Layer.effect(
  MetricsPort,
  Effect.gen(function*() {
    const calls = yield* Ref.make(0)
    return {
      collect: () =>
        Ref.updateAndGet(calls, (n) => n + 1).pipe(
          Effect.map((n) =>
            n <= 1
              ? { errorRate: 0.002, p99LatencyMs: 120 }
              : { errorRate: 0.05, p99LatencyMs: 900 }
          )
        )
    }
  })
)

const DemoHealth = Layer.succeed(HealthPort, {
  check: (params) => Console.log(`  [health]  ${params.service} ${params.version} OK`)
})

const DemoRouter = Layer.succeed(RouterPort, {
  setTrafficWeight: (params) =>
    Console.log(`  [router]  ${params.service} → ${params.version} @ ${params.weight}%`)
})

const DemoNotify = Layer.succeed(NotifyPort, {
  send: (n) => Console.log(`  [notify]  (${n.kind}) ${n.service}: ${n.message}`)
})

const DemoLayer = Layer.mergeAll(DemoMetrics, DemoHealth, DemoRouter, DemoNotify)

const demoInput: DeploymentInput = {
  service: "api",
  version: "v2.1.0",
  previousVersion: "v2.0.8",
  steps: [
    { percent: 10, monitorMs: 300, requiresApproval: false },
    { percent: 50, monitorMs: 300, requiresApproval: false },
    { percent: 100, monitorMs: 0, requiresApproval: false }
  ],
  thresholds: { maxErrorRate: 0.01, maxP99LatencyMs: 500 }
}

const main = async (): Promise<void> => {
  const address = process.env.TEMPORAL_ADDRESS ?? "localhost:7233"
  const namespace = "default"
  const taskQueue = "flux-deployments"

  const runtime = ManagedRuntime.make(DemoLayer)
  const nativeConnection = await NativeConnection.connect({ address })

  const worker = await Worker.create({
    connection: nativeConnection,
    namespace,
    taskQueue,
    workflowsPath: fileURLToPath(import.meta.resolve("@flux/orchestration/workflows")),
    activities: createActivities(runtime)
  })

  try {
    await worker.runUntil(async () => {
      const connection = await Connection.connect({ address })
      const client = new Client({ connection, namespace })
      const workflowId = `demo-${Date.now()}`

      console.log(`\n[flux] deploy api v2.1.0 — strategy canary (10 → 50 → 100)\n`)
      const handle = await client.workflow.start("deploymentWorkflow", {
        taskQueue,
        workflowId,
        args: [demoInput]
      })

      const result = (await handle.result()) as DeploymentResult
      console.log(`\n[flux] outcome: ${JSON.stringify(result)}\n`)
      await connection.close()
    })
  } finally {
    await runtime.dispose()
    await nativeConnection.close()
  }
}

main().catch((error: unknown) => {
  console.error(error)
  process.exit(1)
})
