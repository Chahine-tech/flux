import type { WorkerTuner } from "@temporalio/worker"

/**
 * Worker configuration shared by the process entrypoint (`main.ts`) and the
 * real-cluster tests (D19) — the tests must prove the exact values production
 * runs with, not a copy.
 */

/**
 * Deployment-based Worker Versioning (N4/D15): when a build id is provided,
 * this worker joins a named deployment and pins in-flight workflows to their
 * version, so a rolling upgrade (v1 → v2) never breaks a canary mid-flight —
 * new deployments start on v2, ones already running finish on v1. Left off in
 * dev/tests (no build id), where a versioning-capable server isn't required.
 */
export const versioningOptions = (env: NodeJS.ProcessEnv = process.env) => {
  const buildId = env.FLUX_WORKER_BUILD_ID
  if (buildId === undefined) {
    return undefined
  }
  return {
    version: { deploymentName: env.FLUX_WORKER_DEPLOYMENT ?? "flux-worker", buildId },
    useWorkerVersioning: true as const,
    defaultVersioningBehavior: "PINNED" as const
  }
}

/**
 * Resource-based slot tuning (N4/D18). flux's slot profiles genuinely differ:
 * monitoring is a small number of long-lived, heartbeating activities that each
 * hold a slot for a whole window, so activity slots are capped by *resource
 * pressure* rather than a fixed count that could over-commit memory under a
 * burst of deployments. Health checks are fast local activities, given a wider
 * burst with no ramp throttle.
 */
export const tuner: WorkerTuner = {
  tunerOptions: {
    targetMemoryUsage: Number(process.env.WORKER_TARGET_MEMORY ?? 0.8),
    targetCpuUsage: Number(process.env.WORKER_TARGET_CPU ?? 0.9)
  },
  activityTaskSlotOptions: { minimumSlots: 1, maximumSlots: 200, rampThrottle: "50ms" },
  localActivityTaskSlotOptions: { minimumSlots: 2, maximumSlots: 500, rampThrottle: "0ms" }
}
