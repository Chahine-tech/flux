import { Config, Effect, Layer } from "effect"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { NodeRuntime } from "@effect/platform-node"
import * as Admission from "./admission.ts"
import * as DeploymentEvents from "./deployment-events.ts"
import * as Auth from "./http/auth.ts"
import { serverLayer } from "./http/server.ts"
import * as ReadModel from "./read-model.ts"
import * as TemporalClient from "./temporal-client.ts"

/**
 * flux control plane — HTTP server + real-time events.
 *
 * Composition root: read config, open the Temporal connection (scoped), and
 * launch the HTTP server. `Layer.launch` keeps the process alive until the
 * layer is interrupted, at which point the scoped Temporal connection closes.
 */
const config = Config.all({
  port: Config.Number("PORT").pipe(Config.withDefault(8080)),
  temporalAddress: Config.String("TEMPORAL_ADDRESS").pipe(Config.withDefault("localhost:7233")),
  temporalNamespace: Config.String("TEMPORAL_NAMESPACE").pipe(Config.withDefault("default")),
  pollIntervalMs: Config.Number("POLL_INTERVAL_MS").pipe(Config.withDefault(2000)),
  maxTracked: Config.Number("MAX_TRACKED_DEPLOYMENTS").pipe(Config.withDefault(200)),
  readModelDb: Config.String("READ_MODEL_DB").pipe(Config.withDefault("flux-read-model.db")),
  projectionIntervalMs: Config.Number("PROJECTION_INTERVAL_MS").pipe(Config.withDefault(5000)),
  maxConcurrent: Config.Number("MAX_CONCURRENT_DEPLOYMENTS").pipe(Config.withDefault(10)),
  // Bearer token for the HTTP API; unset → auth disabled (local dev).
  apiToken: Config.Redacted("API_TOKEN").pipe(Config.option)
})

const MainLive = Layer.unwrap(
  Effect.map(config, (cfg) => {
    // The poller releases a deployment's admission slot when it finishes.
    const DeploymentEventsLive = Layer.unwrap(
      Effect.gen(function*() {
        const admission = yield* Admission.AdmissionController
        return DeploymentEvents.layer({
          pollInterval: cfg.pollIntervalMs,
          maxTracked: cfg.maxTracked,
          onDeploymentEnded: (service) => admission.release(service),
          // Re-seat what is already running. `admit` fails with
          // ServiceAlreadyDeploying for the deployments this process itself
          // admitted, which is the expected case and is ignored; it only
          // actually takes a seat after a restart, or for a deployment started
          // through another path.
          onDeploymentSeen: (service) => Effect.ignore(admission.admit(service))
        })
      })
    )
    return serverLayer({ port: cfg.port }).pipe(
      Layer.provide(Auth.layer(cfg.apiToken)),
      Layer.provide(DeploymentEventsLive),
      Layer.provide(ReadModel.layer({ projectionInterval: cfg.projectionIntervalMs, maxProjected: cfg.maxTracked })),
      Layer.provide(SqliteClient.layer({ filename: cfg.readModelDb })),
      Layer.provide(Admission.layer(cfg.maxConcurrent)),
      Layer.provide(TemporalClient.layer({ address: cfg.temporalAddress, namespace: cfg.temporalNamespace }))
    )
  })
)

NodeRuntime.runMain(Layer.launch(MainLive))
