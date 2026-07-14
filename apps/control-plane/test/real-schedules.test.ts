import { Effect } from "effect"
import { Client, Connection } from "@temporalio/client"
import { afterAll, beforeAll, describe, expect, it } from "vitest"
import { deleteDriftSchedule, driftScheduleId, ensureDriftSchedule } from "../src/schedules.ts"

/**
 * Real-cluster proof for Temporal Schedules (D19/D17): the time-skipping test
 * server has no schedule support, so the create → idempotent-update → delete
 * lifecycle runs here against the compose's actual Temporal. Gated by
 * FLUX_REAL_TEMPORAL=1:
 *
 *   docker compose up -d postgresql temporal
 *   FLUX_REAL_TEMPORAL=1 pnpm --filter @flux/control-plane test
 */
const REAL = process.env.FLUX_REAL_TEMPORAL === "1"
const address = process.env.TEMPORAL_ADDRESS ?? "localhost:7233"
const namespace = process.env.TEMPORAL_NAMESPACE ?? "default"

let client: Client

beforeAll(async () => {
  if (!REAL) return
  client = new Client({ connection: await Connection.connect({ address }), namespace })
}, 60_000)

afterAll(async () => {
  await client?.connection.close()
})

describe.skipIf(!REAL)("drift schedules on a real cluster (D19)", () => {
  it("creates the schedule, updates it in place, and deletes it", async () => {
    const service = `drift-test-${Date.now()}`
    const desired = { service, desired: [{ version: "v2", weight: 100 }], reconcile: true }

    // Create.
    const scheduleId = await Effect.runPromise(ensureDriftSchedule(client, { desired, everyMs: 60_000 }))
    expect(scheduleId).toBe(driftScheduleId(service))
    const handle = client.schedule.getHandle(scheduleId)
    const created = await handle.describe()
    expect(created.spec.intervals?.[0]?.every).toBe(60_000)
    expect(created.action.workflowType).toBe("driftCheck")

    // Idempotent re-ensure with a new interval → updated, not duplicated.
    await Effect.runPromise(ensureDriftSchedule(client, { desired, everyMs: 120_000 }))
    const updated = await handle.describe()
    expect(updated.spec.intervals?.[0]?.every).toBe(120_000)

    // Delete, then delete again — both must succeed (idempotent off-switch).
    await Effect.runPromise(deleteDriftSchedule(client, service))
    await expect(handle.describe()).rejects.toThrow()
    await Effect.runPromise(deleteDriftSchedule(client, service))
  })
}, 120_000)
