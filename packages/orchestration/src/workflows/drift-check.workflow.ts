import { log, proxyActivities } from "@temporalio/workflow"
import type { DeploymentActivities } from "../activities/types.ts"
import type { DriftCheckInput, DriftReport, RouteWeight } from "../deployment-input.ts"

/**
 * Drift detection (N4/D17) — deterministic, no Effect.
 *
 * Reads the routing actually in effect for a service and compares it to the
 * desired weights. If they differ, it optionally reconciles by re-applying the
 * desired weights (the same `setTrafficWeight` a deployment uses) and notifies.
 * Meant to run on a Temporal Schedule, one per service.
 */

const acts = proxyActivities<Pick<DeploymentActivities, "readRouterState" | "setTrafficWeight" | "notify">>({
  startToCloseTimeout: "1 minute",
  retry: { maximumAttempts: 3 }
})

const normalize = (weights: ReadonlyArray<RouteWeight>): string =>
  [...weights]
    .filter((w) => Math.round(w.weight) > 0)
    .map((w) => `${w.version}=${Math.round(w.weight)}`)
    .sort()
    .join(",")

/** Desired and actual agree when their non-zero (version → rounded weight) maps match. */
const matches = (desired: ReadonlyArray<RouteWeight>, actual: ReadonlyArray<RouteWeight>): boolean =>
  normalize(desired) === normalize(actual)

export async function driftCheck(input: DriftCheckInput): Promise<DriftReport> {
  const actual = await acts.readRouterState({ service: input.service })

  if (matches(input.desired, actual)) {
    return { service: input.service, drifted: false, reconciled: false, desired: input.desired, actual }
  }

  log.warn("router drift detected", { service: input.service })
  let reconciled = false
  if (input.reconcile) {
    for (const target of input.desired) {
      await acts.setTrafficWeight({ service: input.service, version: target.version, weight: target.weight })
    }
    await acts.notify({
      kind: "step-advanced",
      service: input.service,
      message: `drift reconciled: restored desired routing`
    })
    reconciled = true
  }

  return { service: input.service, drifted: true, reconciled, desired: input.desired, actual }
}
