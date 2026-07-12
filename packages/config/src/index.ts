import { Config, ConfigProvider, Effect, FileSystem, Layer } from "effect"
import { parse as parseToml } from "toml"

/**
 * @flux/config — application configuration from `flux.config.toml`.
 *
 * Built on Effect's `Config` + `ConfigProvider`. Environment variables take
 * precedence over the TOML file, so any value can be overridden at deploy time.
 * Nested paths map to `CONSTANT_CASE` env vars via `constantCase`, e.g.
 * `[temporal].address` → `TEMPORAL_ADDRESS`, `[metrics].prometheus_url` →
 * `METRICS_PROMETHEUS_URL`. Secrets use `Config.redacted` so they never surface
 * in logs or traces.
 */

const temporal = Config.all({
  address: Config.string("address").pipe(Config.withDefault("localhost:7233")),
  namespace: Config.string("namespace").pipe(Config.withDefault("default")),
  taskQueue: Config.string("task_queue").pipe(Config.withDefault("flux-deployments"))
}).pipe(Config.nested("temporal"))

const metrics = Config.all({
  prometheusUrl: Config.string("prometheus_url").pipe(Config.withDefault("http://localhost:9090"))
}).pipe(Config.nested("metrics"))

const router = Config.all({
  type: Config.string("type").pipe(Config.withDefault("nginx")),
  configPath: Config.string("config_path").pipe(
    Config.withDefault("/etc/nginx/conf.d/flux-upstream.conf")
  ),
  reloadCommand: Config.string("reload_command").pipe(Config.withDefault("nginx -s reload"))
}).pipe(Config.nested("router"))

const thresholds = Config.all({
  maxErrorRate: Config.number("max_error_rate").pipe(Config.withDefault(0.01)),
  maxP99LatencyMs: Config.number("max_p99_latency_ms").pipe(Config.withDefault(500))
}).pipe(Config.nested("thresholds"))

const notifications = Config.all({
  slackWebhook: Config.redacted("slack_webhook").pipe(Config.option)
}).pipe(Config.nested("notifications"))

/** The full, typed application configuration. */
export const fluxConfig = Config.all({ temporal, metrics, router, thresholds, notifications })

export type FluxConfig = typeof fluxConfig extends Config.Config<infer A> ? A : never

/**
 * A Layer that installs a ConfigProvider sourced from `path` (TOML), falling
 * back to environment variables. Requires a `FileSystem`.
 */
export const layerFromToml = (
  path: string
): Layer.Layer<never, never, FileSystem.FileSystem> =>
  ConfigProvider.layer(
    Effect.gen(function*() {
      const fs = yield* FileSystem.FileSystem
      const content = yield* fs.readFileString(path).pipe(Effect.orElseSucceed(() => ""))
      const parsed = parseToml(content) as unknown
      const fromToml = ConfigProvider.fromUnknown(parsed)
      const fromEnvironment = ConfigProvider.fromEnv().pipe(ConfigProvider.constantCase)
      // Environment first (override), TOML as the base.
      return ConfigProvider.orElse(fromEnvironment, fromToml)
    })
  )
