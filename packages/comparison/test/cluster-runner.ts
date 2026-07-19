import { appendFileSync } from "node:fs"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { Duration, Effect, Layer } from "effect"
import { HealthPort, MetricsPort, NotifyPort, RouterPort } from "@flux/application"
import type { CanaryStep } from "@flux/domain"
import { ClusterWorkflowEngine, SingleRunner } from "effect/unstable/cluster"
import { DeploymentWorkflow, DeploymentWorkflowLive } from "../src/workflow.ts"

/**
 * Child process for the D23 durability proof — the Effect-side equivalent of
 * what D19's real-cluster CI proves for Temporal. Runs the canary on the
 * durable `ClusterWorkflowEngine` over a SQLite file whose path the test
 * controls. The test SIGKILLs this process mid-monitor, then starts it again
 * on the same file: the same payload derives the same execution id, so the
 * second run must resume the persisted execution — replaying completed
 * activities from SQL instead of re-executing them (the shift log written by
 * the router port is how the test can tell the difference).
 *
 * argv: <sqlite-db-path> <shift-log-path>
 */
const [dbPath, shiftLog] = process.argv.slice(2)
if (!dbPath || !shiftLog) {
  console.error("usage: cluster-runner.ts <db-path> <shift-log-path>")
  process.exit(2)
}

const ports = Layer.mergeAll(
  Layer.succeed(HealthPort, { check: () => Effect.void }),
  Layer.succeed(NotifyPort, { send: () => Effect.void }),
  Layer.succeed(MetricsPort, { query: () => Effect.succeed(0) }),
  Layer.succeed(RouterPort, {
    setTrafficWeight: (params) =>
      Effect.sync(() => {
        appendFileSync(shiftLog, `${params.version}@${params.weight}\n`)
      }),
    readState: () => Effect.succeed([])
  })
)

// The durable engine: cluster workflow engine over the single-node preset,
// message storage in the SQLite file — the state that must survive SIGKILL.
// Runner storage stays in memory: it is cluster topology, ephemeral by nature
// (a restarted process is just a fresh runner acquiring the shards).
const EngineLive = ClusterWorkflowEngine.layer.pipe(
  Layer.provideMerge(Layer.orDie(SingleRunner.layer({
    runnerStorage: "memory",
    // Default is 10s — the resumed process should pick up the unprocessed
    // execution promptly, not after a coffee break.
    shardingConfig: { entityMessagePollInterval: Duration.millis(500) }
  }))),
  Layer.provide(SqliteClient.layer({ filename: dbPath }))
)

const MainLive = DeploymentWorkflowLive.pipe(
  Layer.provide(ports),
  Layer.provideMerge(EngineLive)
)

const steps: readonly [CanaryStep, ...Array<CanaryStep>] = [
  // Long enough that the parent reliably lands its SIGKILL inside it.
  { percent: 10, monitorDuration: Duration.seconds(3), requiresApproval: false },
  { percent: 100, monitorDuration: Duration.zero, requiresApproval: false }
]

// Identical in both phases — the idempotency key (service-version) derives the
// same execution id, which is what makes the second run a resume, not a redo.
const payload = {
  service: "kill-proof",
  version: "v2",
  previousVersion: "v1",
  steps,
  rules: [{ name: "errorRate", query: "q", max: 0.5 }] as const
}

const program = Effect.gen(function*() {
  const result = yield* DeploymentWorkflow.execute(payload)
  console.log(`RESULT:${JSON.stringify(result)}`)
}).pipe(Effect.provide(MainLive))

Effect.runPromise(program).then(
  () => process.exit(0),
  (error) => {
    console.error(error)
    process.exit(1)
  }
)
