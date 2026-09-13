import { Effect, Layer, ManagedRuntime, Option, Redacted } from "effect"
import { Otlp } from "effect/unstable/observability"
import { NodeChildProcessSpawner, NodeFileSystem, NodeHttpClient, NodePath } from "@effect/platform-node"
import {
  AnthropicLanguageModel,
  CaddyRouter,
  GitHubChangelog,
  HttpHealth,
  HttpJsonMetrics,
  NginxRouter,
  PrometheusMetrics,
  SlackNotify
} from "@flux/adapters"
import { fluxConfig, type FluxConfig, layerFromToml } from "@flux/config"
import type { AppServices } from "@flux/orchestration"
import { BoundedHttpClient } from "./http-timeout.ts"

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
  BoundedHttpClient,
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

// Both routers resolve backends the same way: one `service-version` host per
// deployed version (the compose topology). `versionOf` is the inverse — how the
// Caddy adapter names the versions it reads back from the live config.
const backendAddress = (service: string, version: string): string => `${service}-${version}:8080`
const versionOfDial = (service: string, dial: string): string | undefined =>
  dial.startsWith(`${service}-`) ? dial.slice(service.length + 1).replace(/:\d+$/, "") : undefined

/** Select the MetricsPort adapter the config asks for. */
const metricsLayer = (config: FluxConfig) => {
  switch (config.metrics.type) {
    case "prometheus":
      return PrometheusMetrics.layer({ url: config.metrics.prometheusUrl })
    case "http-json":
      return HttpJsonMetrics.layer({ authToken: config.metrics.httpJsonToken })
    default:
      throw new Error(`metrics.type "${config.metrics.type}" is not implemented — use "prometheus" or "http-json"`)
  }
}

/** Select the RouterPort adapter the config asks for. */
const routerLayer = (config: FluxConfig) => {
  switch (config.router.type) {
    case "nginx":
      return NginxRouter.layer({
        configPath: config.router.configPath,
        reloadCommand: config.router.reloadCommand.split(/\s+/) as [string, ...ReadonlyArray<string>],
        address: backendAddress
      })
    case "caddy":
      return CaddyRouter.layer({
        adminUrl: config.router.adminUrl,
        server: config.router.serverName,
        address: backendAddress,
        versionOf: versionOfDial
      })
    default:
      // Refusing unknown types at startup keeps the config honest — a silently
      // ignored `router.type` would be worse than none.
      throw new Error(`router.type "${config.router.type}" is not implemented — use "nginx" or "caddy"`)
  }
}

const CoreLayer: Layer.Layer<AppServices> = Layer.unwrap(
  Effect.gen(function*() {
    // A malformed flux.config.toml is a fatal startup defect, not recoverable.
    const config = yield* Effect.orDie(fluxConfig)

    return Layer.mergeAll(
      metricsLayer(config),
      HttpHealth.layer({
        // Default targets a `service-version` host — the compose         // topology where each version is its own container. HEALTH_URL overrides
        // it with a fixed URL, used by the local demo where the worker runs on
        // the host and probes a single stand-in target.
        url: process.env.HEALTH_URL
          ? () => process.env.HEALTH_URL as string
          : ({ service, version }) => `http://${service}-${version}:8080/health`
      }),
      SlackNotify.layer({
        webhookUrl: Option.getOrElse(config.notifications.slackWebhook, () => Redacted.make(""))
      }),
      // The LanguageModel port for the rollback postmortem. An empty key
      // (the default) makes it no-op without a network call.
      AnthropicLanguageModel.layer({
        apiKey: config.ai.anthropicApiKey,
        model: config.ai.model,
        baseUrl: config.ai.baseUrl
      }),
      // What grounds that postmortem: the commits between the two versions. An
      // empty repo template disables it and the postmortem stays metrics-only.
      GitHubChangelog.layer({
        repoTemplate: config.changelog.repoTemplate,
        token: config.changelog.githubToken,
        baseUrl: config.changelog.baseUrl
      }),
      routerLayer(config)
    ).pipe(Layer.provide(PlatformLayer))
  })
).pipe(
  Layer.provide(ConfigLayer),
  Layer.provide(NodeFileSystem.layer)
)

export const AppLayer: Layer.Layer<AppServices> = Layer.mergeAll(CoreLayer, TracingLayer)

export const makeRuntime = (): ManagedRuntime.ManagedRuntime<AppServices, never> =>
  ManagedRuntime.make(AppLayer)
