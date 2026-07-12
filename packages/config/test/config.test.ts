import { describe, it } from "@effect/vitest"
import { Config, ConfigProvider, Effect, Option, Redacted } from "effect"
import { parse as parseToml } from "toml"
import { expect } from "vitest"
import { fluxConfig } from "../src/index.ts"

const TOML = `
[temporal]
address = "temporal.internal:7233"
namespace = "prod"

[metrics]
prometheus_url = "http://prom:9090"

[thresholds]
max_error_rate = 0.02
max_p99_latency_ms = 300

[router]
config_path = "/tmp/flux.conf"
reload_command = "nginx -s reload"

[notifications]
slack_webhook = "https://hooks.slack.com/xxx"
`

const withToml = (toml: string) =>
  Effect.provideService(
    fluxConfig,
    ConfigProvider.ConfigProvider,
    ConfigProvider.fromUnknown(parseToml(toml))
  )

describe("fluxConfig", () => {
  it.effect("reads nested TOML tables", () =>
    Effect.gen(function*() {
      const config = yield* withToml(TOML)
      expect(config.temporal.address).toBe("temporal.internal:7233")
      expect(config.temporal.namespace).toBe("prod")
      expect(config.metrics.prometheusUrl).toBe("http://prom:9090")
      expect(config.thresholds.maxErrorRate).toBeCloseTo(0.02)
      expect(config.thresholds.maxP99LatencyMs).toBe(300)
      expect(config.router.configPath).toBe("/tmp/flux.conf")
    }))

  it.effect("applies defaults for absent keys", () =>
    Effect.gen(function*() {
      const config = yield* withToml(`[temporal]\naddress = "x:7233"\n`)
      expect(config.temporal.namespace).toBe("default")
      expect(config.temporal.taskQueue).toBe("flux-deployments")
      expect(config.thresholds.maxErrorRate).toBeCloseTo(0.01)
      expect(config.router.reloadCommand).toBe("nginx -s reload")
    }))

  it.effect("keeps the slack webhook Redacted and optional", () =>
    Effect.gen(function*() {
      const withSecret = yield* withToml(TOML)
      expect(Option.isSome(withSecret.notifications.slackWebhook)).toBe(true)
      if (Option.isSome(withSecret.notifications.slackWebhook)) {
        expect(Redacted.value(withSecret.notifications.slackWebhook.value)).toContain("hooks.slack.com")
        // A Redacted must not leak its value when stringified.
        expect(String(withSecret.notifications.slackWebhook.value)).not.toContain("hooks.slack.com")
      }

      const withoutSecret = yield* withToml(`[temporal]\naddress = "x:7233"\n`)
      expect(Option.isNone(withoutSecret.notifications.slackWebhook)).toBe(true)
    }))

  it.effect("lets an environment variable override the TOML value", () =>
    Effect.gen(function*() {
      // Nested path [temporal].address maps to TEMPORAL_ADDRESS via constantCase.
      const env = ConfigProvider.fromEnv({ env: { TEMPORAL_ADDRESS: "env-host:9999" } }).pipe(
        ConfigProvider.constantCase
      )
      const toml = ConfigProvider.fromUnknown(parseToml(TOML))
      const config = yield* fluxConfig.pipe(
        Effect.provideService(ConfigProvider.ConfigProvider, ConfigProvider.orElse(env, toml))
      )
      expect(config.temporal.address).toBe("env-host:9999")
      // Values not set in env still come from TOML.
      expect(config.metrics.prometheusUrl).toBe("http://prom:9090")
    }))
})
