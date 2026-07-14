import { Effect, Layer, ManagedRuntime, Option, Redacted } from "effect"
import { Otlp } from "effect/unstable/observability"
import { NodeChildProcessSpawner, NodeFileSystem, NodeHttpClient, NodePath } from "@effect/platform-node"
import { HttpHealth, NginxRouter, PrometheusMetrics, SlackNotify } from "@flux/adapters"
import { fluxConfig, layerFromToml } from "@flux/config"
import type { AppServices } from "@flux/orchestration"

/**
 * Composition root for the worker.
 *
 * Reads `flux.config.toml` (env-overridable) through @flux/config, then builds
 * the 4 adapter Layers from it and provides them the Node platform Layers
 * (HttpClient / FileSystem / ChildProcessSpawner). The resulting `AppLayer`
 * provides exactly the 4 ports the activities need.
 */

// NodeChildProcessSpawner requires FileSystem + Path, so feed those to it locally.
const SpawnerLayer = NodeChildProcessSpawner.layer.pipe(
  Layer.provide(Layer.mergeAll(NodeFileSystem.layer, NodePath.layer))
)

const PlatformLayer = Layer.mergeAll(
  NodeHttpClient.layerUndici,
  NodeFileSystem.layer,
  SpawnerLayer
)

const ConfigLayer = layerFromToml(process.env.FLUX_CONFIG ?? "flux.config.toml")

// Export Effect spans (activities, use cases, Prometheus HTTP calls) via OTLP.
// Effect v4 has a native OTLP tracer — no external OpenTelemetry SDK needed.
const TracingLayer = Otlp.layerJson({
  baseUrl: process.env.OTLP_ENDPOINT ?? "http://localhost:4318",
  resource: { serviceName: "flux-worker" }
}).pipe(Layer.provide(NodeHttpClient.layerUndici))

const CoreLayer: Layer.Layer<AppServices> = Layer.unwrap(
  Effect.gen(function*() {
    // A malformed flux.config.toml is a fatal startup defect, not recoverable.
    const config = yield* Effect.orDie(fluxConfig)
    const reloadCommand = config.router.reloadCommand.split(/\s+/) as [string, ...ReadonlyArray<string>]

    return Layer.mergeAll(
      PrometheusMetrics.layer({ url: config.metrics.prometheusUrl }),
      HttpHealth.layer({
        // Default targets a `service-version` host — the compose / N0-e2e
        // topology where each version is its own container. HEALTH_URL overrides
        // it with a fixed URL, used by the local demo where the worker runs on
        // the host and probes a single stand-in target.
        url: process.env.HEALTH_URL
          ? () => process.env.HEALTH_URL as string
          : ({ service, version }) => `http://${service}-${version}:8080/health`
      }),
      SlackNotify.layer({
        webhookUrl: Option.getOrElse(config.notifications.slackWebhook, () => Redacted.make(""))
      }),
      NginxRouter.layer({
        configPath: config.router.configPath,
        reloadCommand,
        address: (service, version) => `${service}-${version}:8080`
      })
    ).pipe(Layer.provide(PlatformLayer))
  })
).pipe(
  Layer.provide(ConfigLayer),
  Layer.provide(NodeFileSystem.layer)
)

export const AppLayer: Layer.Layer<AppServices> = Layer.mergeAll(CoreLayer, TracingLayer)

export const makeRuntime = (): ManagedRuntime.ManagedRuntime<AppServices, never> =>
  ManagedRuntime.make(AppLayer)
