import { Duration } from "effect"
import type { DeploymentConfig, RolloutPlan } from "@flux/domain"
import type { DeploymentInput, DeploymentStrategy, RolloutPlanInput } from "./deployment-input.ts"

/**
 * `deployment-input.ts` imports nothing, so it restates the compiled plan's
 * shape instead of importing `RolloutPlan` from the domain. This assignment is
 * the guard against the two drifting apart: it is erased at runtime and fails
 * the build the moment the domain's plan stops fitting what the workflow reads.
 */
const _planShapesAgree: (plan: RolloutPlan) => RolloutPlanInput = (plan) => plan
void _planShapesAgree

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
    max: rule.max,
    // Carried through, or the rule silently loses the thing that lets it say
    // it does not know yet.
    sampleSize: rule.sampleSize
  })),
  // Operational default; a per-deployment override can come from config later.
  pollIntervalMs: 5_000
})
