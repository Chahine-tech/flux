import { Layer, ManagedRuntime, Redacted } from "effect"
import { NodeChildProcessSpawner, NodeFileSystem, NodeHttpClient, NodePath } from "@effect/platform-node"
import { HttpHealth, NginxRouter, PrometheusMetrics, SlackNotify } from "@flux/adapters"
import type { AppServices } from "@flux/orchestration"

/**
 * Composition root for the worker (ARCHITECTURE.md D7).
 *
 * The 4 adapter Layers are composed once and provided the Node platform Layers
 * (HttpClient / FileSystem / ChildProcessSpawner) they depend on. The resulting
 * `AppLayer` provides exactly the 4 ports the activities need.
 *
 * Config is read from the environment for N0; N1 replaces this with an Effect
 * `ConfigProvider` backed by `flux.config.toml`.
 */

const env = (key: string, fallback: string): string => process.env[key] ?? fallback

// NodeChildProcessSpawner requires FileSystem + Path, so feed those to it locally.
const SpawnerLayer = NodeChildProcessSpawner.layer.pipe(
  Layer.provide(Layer.mergeAll(NodeFileSystem.layer, NodePath.layer))
)

const PlatformLayer = Layer.mergeAll(
  NodeHttpClient.layerUndici,
  NodeFileSystem.layer,
  SpawnerLayer
)

const AdaptersLayer = Layer.mergeAll(
  PrometheusMetrics.layer({ url: env("PROMETHEUS_URL", "http://localhost:9090") }),
  HttpHealth.layer({
    url: ({ service, version }) => `http://${service}-${version}:8080/health`
  }),
  SlackNotify.layer({ webhookUrl: Redacted.make(env("SLACK_WEBHOOK_URL", "")) }),
  NginxRouter.layer({
    configPath: env("NGINX_CONFIG_PATH", "/etc/nginx/conf.d/flux-upstream.conf"),
    reloadCommand: ["nginx", "-s", "reload"],
    address: (service, version) => `${service}-${version}:8080`
  })
)

export const AppLayer: Layer.Layer<AppServices> = AdaptersLayer.pipe(Layer.provide(PlatformLayer))

export const makeRuntime = (): ManagedRuntime.ManagedRuntime<AppServices, never> =>
  ManagedRuntime.make(AppLayer)
