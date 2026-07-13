import { describe, it } from "@effect/vitest"
import { Effect, Layer } from "effect"
import { TestClock } from "effect/testing"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { expect } from "vitest"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { layer as readModelLayer, ReadModel } from "../src/read-model.ts"
import type { ClosedDeployment } from "../src/temporal-client.ts"
import { TemporalClient } from "../src/temporal-client.ts"

/**
 * Runs the read model against a real `node:sqlite` database (in-process, no
 * cluster): a fake `TemporalClient` feeds canned closed deployments, the
 * projection writes them, and `stats()` runs the actual aggregation SQL. This
 * covers what the mocked HTTP test can't — the migration, the idempotent
 * `INSERT OR IGNORE`, and the `GROUP BY` / `CASE` / `AVG` query itself.
 */

const closed: ReadonlyArray<ClosedDeployment> = [
  { workflowId: "c-1", service: "checkout", status: "Succeeded", durationMs: 30_000 },
  { workflowId: "c-2", service: "checkout", status: "Succeeded", durationMs: 50_000 },
  { workflowId: "c-3", service: "checkout", status: "RolledBack", durationMs: 10_000 },
  { workflowId: "a-1", service: "api", status: "Failed", durationMs: 8_000 }
]

const FakeTemporal = Layer.succeed(TemporalClient, {
  start: () => Effect.succeed("id"),
  status: () => Effect.die("unused"),
  list: () => Effect.succeed([]),
  listRunningIds: () => Effect.succeed([]),
  // Returned repeatedly — the projection must stay idempotent across ticks.
  listClosed: () => Effect.succeed(closed),
  approve: () => Effect.void,
  abort: () => Effect.void
})

const makeLayer = () => {
  const filename = join(tmpdir(), `flux-read-model-test-${Date.now()}-${Math.random().toString(36).slice(2)}.db`)
  return readModelLayer({ projectionInterval: "5 seconds", maxProjected: 100 }).pipe(
    Layer.provide(FakeTemporal),
    Layer.provide(SqliteClient.layer({ filename }))
  )
}

describe("read model", () => {
  it.effect("projects closed deployments and aggregates per service", () =>
    Effect.gen(function*() {
      const readModel = yield* ReadModel
      // Let the projection run twice — the second tick must not double-count.
      yield* TestClock.adjust("6 seconds")

      const stats = yield* readModel.stats()
      const byService = Object.fromEntries(stats.map((s) => [s.service, s]))

      expect(byService["checkout"]).toMatchObject({
        total: 3,
        succeeded: 2,
        rolledBack: 1,
        failed: 0,
        meanDurationMs: 30_000
      })
      expect(byService["checkout"]?.rollbackRate).toBeCloseTo(1 / 3)
      expect(byService["api"]).toMatchObject({ total: 1, failed: 1, rollbackRate: 0 })
    }).pipe(Effect.provide(makeLayer())))
})
