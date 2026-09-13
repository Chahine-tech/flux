import { Console, Effect, Option, Schema } from "effect"
import { Command, Flag } from "effect/unstable/cli"
import { PrometheusMetrics } from "@flux/adapters"
import type { TriggerDeploymentRequest } from "@flux/contracts"
import { DeploymentConfig } from "@flux/domain"
import { configToInput } from "@flux/orchestration"
import { clientLayer, makeClient } from "../control-plane.ts"
import { tracedCommand } from "../tracing.ts"

/**
 * `flux deploy` — start a canary deployment.
 *
 * Flags build a raw config that is validated through the domain `Schema`, mapped
 * to the workflow input, and sent to the control plane's `POST /deployments`
 * — so the deployment passes the admission controller (global concurrency
 * budget, one deployment per service) instead of starting the workflow directly.
 * Canary steps and thresholds use built-in defaults.
 */
export const deploy = Command.make("deploy", {
  service: Flag.String("service").pipe(Flag.withDescription("Service name")),
  version: Flag.String("version").pipe(Flag.withDescription("New version to roll out")),
  previousVersion: Flag.String("previous-version").pipe(
    Flag.withDescription("Version to roll back to on regression")
  ),
  monitor: Flag.String("monitor").pipe(
    Flag.withDefault("30s"),
    Flag.withDescription("Monitoring window per canary step")
  ),
  strategy: Flag.String("strategy").pipe(
    Flag.withDefault("canary"),
    Flag.withDescription('Rollout strategy: "canary" (default) or "blue-green"')
  ),
  bake: Flag.String("bake").pipe(
    Flag.withDefault("30s"),
    Flag.withDescription("Bake window after the blue/green flip (only for --strategy blue-green)")
  ),
  window: Flag.String("window").pipe(
    Flag.optional,
    Flag.withDescription('Only deploy inside this cron window, e.g. "* 9-17 * * 1-5"')
  ),
  controlPlane: Flag.String("control-plane").pipe(
    Flag.withDefault("http://localhost:8080"),
    Flag.withDescription("Control plane base URL")
  )
}, (config) =>
  Effect.gen(function*() {
    const raw = {
      service: config.service,
      version: config.version,
      previousVersion: config.previousVersion,
      strategy: config.strategy === "blue-green"
        ? { _tag: "blue-green", bakeDuration: config.bake, requiresApproval: false }
        : {
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
    const client = yield* makeClient(config.controlPlane)
    // The workflow-facing input is structurally the request; the server revalidates it.
    const window = Option.getOrUndefined(config.window)
    const payload = {
      ...configToInput(decoded),
      ...(window === undefined ? {} : { window })
    } as TriggerDeploymentRequest
    const { workflowId } = yield* client.deployments.trigger({ payload })

    yield* Console.log(
      `[flux] started deployment ${workflowId} — ${config.service} → ${config.version} (${
        config.strategy === "blue-green" ? "blue/green" : "canary 10→50→100"
      })`
    )
  }).pipe(
    Effect.provide(clientLayer),
    Effect.catchTag("DeploymentBudgetExhausted", (error) =>
      Console.error(`[flux] rejected: concurrency budget full (limit ${error.limit})`)),
    Effect.catchTag("ServiceAlreadyDeploying", (error) =>
      Console.error(`[flux] rejected: ${error.service} already has a deployment in flight`)),
    Effect.catchTag("OutsideDeploymentWindow", (error) =>
      Console.error(`[flux] rejected: outside deploy window "${error.window}" — next opens ${error.nextAllowed}`)),
    Effect.catchTag("TemporalUnavailable", (error) =>
      Console.error(
        `[flux] the control plane could not reach Temporal (${error.operation}): ${error.detail}` +
          ", nothing was started, retry when it is back"
      )),
    tracedCommand("deploy")
  ))
