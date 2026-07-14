import { Effect } from "effect"
import type { DriftCheckInput } from "@flux/orchestration"
import type { Client } from "@temporalio/client"

/**
 * Drift-detection schedules (N4/D17). A Temporal Schedule runs the `driftCheck`
 * workflow for one service on a fixed interval, comparing the router's actual
 * routing to the desired weights and reconciling on drift. Meant to be created
 * (or updated) when a deployment succeeds, so the desired state stays current.
 */

const TASK_QUEUE = "flux-deployments"

export interface DriftScheduleOptions {
  readonly desired: DriftCheckInput
  readonly everyMs: number
}

/** The stable schedule id for a service's drift check. */
export const driftScheduleId = (service: string): string => `flux-drift-${service}`

/**
 * Create the drift-check schedule for a service, or update its desired state and
 * interval if it already exists — idempotent, so it is safe to call on every
 * successful deployment.
 */
export const ensureDriftSchedule = (client: Client, options: DriftScheduleOptions): Effect.Effect<string> =>
  Effect.promise(async () => {
    const scheduleId = driftScheduleId(options.desired.service)
    const spec = { intervals: [{ every: `${options.everyMs}ms` }] }
    const action = {
      type: "startWorkflow" as const,
      workflowType: "driftCheck",
      taskQueue: TASK_QUEUE,
      workflowId: `${scheduleId}-run`,
      args: [options.desired]
    }
    try {
      await client.schedule.create({ scheduleId, spec, action })
    } catch {
      // Already exists → keep the desired state and interval current.
      await client.schedule.getHandle(scheduleId).update((previous) => ({ ...previous, spec, action }))
    }
    return scheduleId
  })
