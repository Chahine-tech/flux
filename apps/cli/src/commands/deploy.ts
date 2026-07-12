import { Console, Effect, Schema } from "effect"
import { Command, Flag } from "effect/unstable/cli"
import { PrometheusMetrics } from "@flux/adapters"
import { DeploymentConfig } from "@flux/domain"
import { configToInput } from "@flux/orchestration"
import { startDeployment } from "../temporal.ts"

/**
 * `flux deploy` — start a canary deployment.
 *
 * Flags build a raw config that is validated through the domain `Schema`
 * (bad percentages / thresholds are rejected here), mapped to the Effect-free
 * workflow input, and used to start the Temporal workflow. Canary steps and
 * thresholds use built-in defaults for N0; loading them from `flux.config.toml`
 * via a ConfigProvider is N1.
 */
export const deploy = Command.make("deploy", {
  service: Flag.string("service").pipe(Flag.withDescription("Service name")),
  version: Flag.string("version").pipe(Flag.withDescription("New version to roll out")),
  previousVersion: Flag.string("previous-version").pipe(
    Flag.withDescription("Version to roll back to on regression")
  ),
  monitor: Flag.string("monitor").pipe(
    Flag.withDefault("30s"),
    Flag.withDescription("Monitoring window per canary step")
  )
}, (config) =>
  Effect.gen(function*() {
    const raw = {
      service: config.service,
      version: config.version,
      previousVersion: config.previousVersion,
      strategy: {
        _tag: "canary",
        steps: [
          { percent: 10, monitorDuration: config.monitor, requiresApproval: false },
          { percent: 50, monitorDuration: config.monitor, requiresApproval: false },
          { percent: 100, monitorDuration: "0s", requiresApproval: false }
        ]
      },
      thresholds: [
        { name: "errorRate", query: PrometheusMetrics.errorRateQuery(config.service), max: 0.01 },
        { name: "p99", query: PrometheusMetrics.p99LatencyQuery(config.service), max: 500 }
      ]
    }

    const decoded = yield* Schema.decodeUnknownEffect(DeploymentConfig)(raw)
    const workflowId = yield* Effect.promise(() => startDeployment(configToInput(decoded)))

    yield* Console.log(
      `[flux] started deployment ${workflowId} — ${config.service} → ${config.version} (canary 10→50→100)`
    )
  }))
