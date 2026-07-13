import { Config, Effect, Layer } from "effect"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { NodeRuntime } from "@effect/platform-node"
import * as DeploymentEvents from "./deployment-events.ts"
import { serverLayer } from "./http/server.ts"
import * as ReadModel from "./read-model.ts"
import * as TemporalClient from "./temporal-client.ts"

/**
 * flux control plane — HTTP server + real-time events (level N3).
 *
 * Composition root: read config, open the Temporal connection (scoped), and
 * launch the HTTP server. `Layer.launch` keeps the process alive until the
 * layer is interrupted, at which point the scoped Temporal connection closes.
 */
const config = Config.all({
  port: Config.number("PORT").pipe(Config.withDefault(8080)),
  temporalAddress: Config.string("TEMPORAL_ADDRESS").pipe(Config.withDefault("localhost:7233")),
  temporalNamespace: Config.string("TEMPORAL_NAMESPACE").pipe(Config.withDefault("default")),
  pollIntervalMs: Config.number("POLL_INTERVAL_MS").pipe(Config.withDefault(2000)),
  maxTracked: Config.number("MAX_TRACKED_DEPLOYMENTS").pipe(Config.withDefault(200)),
  readModelDb: Config.string("READ_MODEL_DB").pipe(Config.withDefault("flux-read-model.db")),
  projectionIntervalMs: Config.number("PROJECTION_INTERVAL_MS").pipe(Config.withDefault(5000))
})

const MainLive = Layer.unwrap(
  Effect.map(config, (cfg) =>
    serverLayer({ port: cfg.port }).pipe(
      Layer.provide(DeploymentEvents.layer({ pollInterval: cfg.pollIntervalMs, maxTracked: cfg.maxTracked })),
      Layer.provide(ReadModel.layer({ projectionInterval: cfg.projectionIntervalMs, maxProjected: cfg.maxTracked })),
      Layer.provide(SqliteClient.layer({ filename: cfg.readModelDb })),
      Layer.provide(TemporalClient.layer({ address: cfg.temporalAddress, namespace: cfg.temporalNamespace }))
    ))
)

NodeRuntime.runMain(Layer.launch(MainLive))
