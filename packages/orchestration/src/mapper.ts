import { Duration } from "effect"
import type { DeploymentConfig } from "@flux/domain"
import type { DeploymentInput } from "./deployment-input.ts"

/**
 * Convert a domain `DeploymentConfig` (Effect `Duration` values) into the
 * Effect-free `DeploymentInput` a workflow consumes (durations as milliseconds).
 * Runs on the Effect side (CLI / client) before starting a workflow — never
 * imported by the workflow bundle.
 */
export const configToInput = (config: DeploymentConfig): DeploymentInput => ({
  service: config.service,
  version: config.version,
  previousVersion: config.previousVersion,
  steps: config.strategy.steps.map((step) => ({
    percent: step.percent,
    monitorMs: Duration.toMillis(step.monitorDuration),
    requiresApproval: step.requiresApproval,
    ...(step.approvalTimeout !== undefined
      ? { approvalTimeoutMs: Duration.toMillis(step.approvalTimeout) }
      : {})
  })),
  rules: config.thresholds.map((rule) => ({
    name: rule.name,
    query: rule.query,
    max: rule.max
  })),
  // Operational default; a per-deployment override can come from config later.
  pollIntervalMs: 5_000
})
