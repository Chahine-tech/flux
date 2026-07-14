import { Console, Effect, Layer, ManagedRuntime, Ref } from "effect"
import { Client, Connection } from "@temporalio/client"
import { NativeConnection, Worker } from "@temporalio/worker"
import { fileURLToPath } from "node:url"
import { HealthPort, MetricsPort, NotifyPort, RouterPort } from "@flux/application"
import { createActivities, type DeploymentInput, type DeploymentResult, makePayloadCodec } from "@flux/orchestration"

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

// Shared canary state: the new version's current traffic weight. The demo
// models "the new version is bad": once >= 50% of traffic hits it, metrics
// breach — deterministic regardless of how many times the Stream polls.
const makeDemoLayer = Effect.gen(function*() {
  const weights = yield* Ref.make<Record<string, number>>({})

  const DemoRouter = Layer.succeed(RouterPort, {
    setTrafficWeight: (params) =>
      Ref.update(weights, (w) => ({ ...w, [params.version]: params.weight })).pipe(
        Effect.andThen(
          Console.log(`  [router]  ${params.service} → ${params.version} @ ${params.weight}%`)
        )
      ),
    readState: () =>
      Ref.get(weights).pipe(
        Effect.map((w) => Object.entries(w).map(([version, weight]) => ({ version, weight })))
      )
  })

  // The new version's error rate rises once it takes >= 50% of traffic. The
  // query string is ignored — both demo rules share it, so the RequestResolver
  // fetches it once per poll.
  const DemoMetrics = Layer.succeed(MetricsPort, {
    query: () =>
      Ref.get(weights).pipe(Effect.map((w) => ((w["v2.1.0"] ?? 0) >= 50 ? 0.08 : 0.002)))
  })

  const DemoHealth = Layer.succeed(HealthPort, {
    check: (params) => Console.log(`  [health]  ${params.service} ${params.version} OK`)
  })

  const DemoNotify = Layer.succeed(NotifyPort, {
    send: (n) => Console.log(`  [notify]  (${n.kind}) ${n.service}: ${n.message}`)
  })

  return Layer.mergeAll(DemoMetrics, DemoHealth, DemoRouter, DemoNotify)
})

const demoInput: DeploymentInput = {
  service: "api",
  version: "v2.1.0",
  previousVersion: "v2.0.8",
  steps: [
    { percent: 10, monitorMs: 300, requiresApproval: false },
    { percent: 50, monitorMs: 300, requiresApproval: false },
    { percent: 100, monitorMs: 0, requiresApproval: false }
  ],
  // Two rules share the same query -> deduped to one fetch per poll.
  rules: [
    { name: "errorRate", query: "flux_demo_error_rate", max: 0.01 },
    { name: "errorRateWarn", query: "flux_demo_error_rate", max: 0.05 }
  ],
  pollIntervalMs: 100
}

const main = async (): Promise<void> => {
  const address = process.env.TEMPORAL_ADDRESS ?? "localhost:7233"
  const namespace = "default"
  const taskQueue = "flux-deployments"

  const runtime = ManagedRuntime.make(Layer.unwrap(makeDemoLayer))
  const nativeConnection = await NativeConnection.connect({ address })

  const worker = await Worker.create({
    connection: nativeConnection,
    namespace,
    taskQueue,
    workflowsPath: fileURLToPath(import.meta.resolve("@flux/orchestration/workflows")),
    activities: createActivities(runtime),
    dataConverter: { payloadCodecs: [makePayloadCodec()] }
  })

  try {
    await worker.runUntil(async () => {
      const connection = await Connection.connect({ address })
      const client = new Client({
        connection,
        namespace,
        dataConverter: { payloadCodecs: [makePayloadCodec()] }
      })
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
