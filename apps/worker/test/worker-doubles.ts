import { appendFileSync } from "node:fs"
import { Context } from "@temporalio/activity"
import type { DeploymentActivities } from "@flux/orchestration"

/**
 * Side-effect-logging activity doubles shared by the D27 worker-kill proof:
 * the child worker (`worker-runner.ts`) and the in-process successor worker
 * append to the same file, which is how the test distinguishes "replayed from
 * history" (line appears once) from "re-executed" (line appears again).
 */
export const activities = (log: string): DeploymentActivities => ({
  healthCheck: async () => {},
  setTrafficWeight: async (params: { version: string; weight: number }) => {
    appendFileSync(log, `${params.version}@${params.weight}\n`)
  },
  // Long and heartbeating, like the real monitor — the heartbeat is what lets
  // the server notice a dead worker (heartbeatTimeout, set by the workflow at
  // 30s) and reschedule the activity onto the next worker.
  monitorStep: async () => {
    appendFileSync(log, "monitor-start\n")
    for (let i = 0; i < 8; i++) {
      Context.current().heartbeat()
      await new Promise((resolve) => setTimeout(resolve, 1000))
    }
    return { _tag: "Within" as const }
  },
  notify: async () => {},
  readRouterState: async () => [],
  recordOutcome: async () => {}
})
