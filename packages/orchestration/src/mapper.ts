import { Duration } from "effect"
import type { DeploymentConfig } from "@flux/domain"
import type { DeploymentInput, DeploymentStrategy } from "./deployment-input.ts"

/**
 * Convert a domain `DeploymentConfig` (Effect `Duration` values) into the
 * Effect-free `DeploymentInput` a workflow consumes (durations as milliseconds).
 * Runs on the Effect side (CLI / client) before starting a workflow — never
 * imported by the workflow bundle.
 */
const toStrategy = (strategy: DeploymentConfig["strategy"]): DeploymentStrategy => {
  switch (strategy._tag) {
    case "canary":
      return {
        kind: "canary",
        steps: strategy.steps.map((step) => ({
          percent: step.percent,
          monitorMs: Duration.toMillis(step.monitorDuration),
          requiresApproval: step.requiresApproval,
          ...(step.approvalTimeout !== undefined
            ? { approvalTimeoutMs: Duration.toMillis(step.approvalTimeout) }
            : {})
        }))
      }
    case "blue-green":
      return {
        kind: "blue-green",
        bakeMs: Duration.toMillis(strategy.bakeDuration),
        requiresApproval: strategy.requiresApproval,
        ...(strategy.approvalTimeout !== undefined
          ? { approvalTimeoutMs: Duration.toMillis(strategy.approvalTimeout) }
          : {})
      }
  }
}

export const configToInput = (config: DeploymentConfig): DeploymentInput => ({
  service: config.service,
  version: config.version,
  previousVersion: config.previousVersion,
  strategy: toStrategy(config.strategy),
  rules: config.thresholds.map((rule) => ({
    name: rule.name,
    query: rule.query,
    max: rule.max
  })),
  // Operational default; a per-deployment override can come from config later.
  pollIntervalMs: 5_000
})
