import { Context, type Duration, Effect, Layer, Schedule } from "effect"
import { SqlClient } from "effect/unstable/sql"
import type { ServiceStats } from "@flux/contracts"
import { TemporalClient } from "./temporal-client.ts"

/**
 * CQRS read model for the control plane (N3, D12).
 *
 * Temporal advanced visibility lists and filters deployments but cannot
 * aggregate (no `GROUP BY`, no averages), so a poller projects finished
 * deployments into a local SQLite table and `GET /stats` answers the questions
 * visibility can't: rollback rate per service, mean canary duration. It is a
 * pure projection — Temporal stays the source of truth, so the table can be
 * rebuilt at any time and the projection is idempotent (`INSERT OR IGNORE`).
 */
export class ReadModel extends Context.Service<ReadModel, {
  readonly stats: () => Effect.Effect<ReadonlyArray<ServiceStats>>
}>()("ReadModel") {}

export interface ReadModelConfig {
  /** How often the projection scans Temporal for newly-finished deployments. */
  readonly projectionInterval: Duration.Input
  /** Upper bound on deployments projected per scan. */
  readonly maxProjected: number
}

interface StatsRow {
  readonly service: string
  readonly total: number
  readonly succeeded: number
  readonly rolledBack: number
  readonly aborted: number
  readonly failed: number
  readonly meanDurationMs: number
}

export const layer = (
  config: ReadModelConfig
): Layer.Layer<ReadModel, never, SqlClient.SqlClient | TemporalClient> =>
  Layer.effect(
    ReadModel,
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const temporal = yield* TemporalClient

      // A read-model schema failure is a startup defect, not a recoverable error.
      yield* sql`
        CREATE TABLE IF NOT EXISTS deployments (
          workflow_id TEXT PRIMARY KEY,
          service TEXT NOT NULL,
          status TEXT NOT NULL,
          duration_ms INTEGER NOT NULL
        )
      `.pipe(Effect.orDie)

      // Idempotent projection: finished deployments are inserted once, keyed by id.
      const project = Effect.gen(function*() {
        const closed = yield* temporal.listClosed(config.maxProjected)
        yield* Effect.forEach(
          closed,
          (deployment) =>
            sql`
              INSERT OR IGNORE INTO deployments (workflow_id, service, status, duration_ms)
              VALUES (${deployment.workflowId}, ${deployment.service}, ${deployment.status}, ${deployment.durationMs})
            `,
          { discard: true }
        )
      })

      yield* Effect.forkScoped(
        project.pipe(
          Effect.catchCause((cause) => Effect.logWarning("read-model projection failed", cause)),
          Effect.repeat(Schedule.spaced(config.projectionInterval))
        )
      )

      const stats = (): Effect.Effect<ReadonlyArray<ServiceStats>> =>
        sql<StatsRow>`
          SELECT
            service,
            COUNT(*) AS total,
            SUM(CASE WHEN status = 'Succeeded' THEN 1 ELSE 0 END) AS succeeded,
            SUM(CASE WHEN status = 'RolledBack' THEN 1 ELSE 0 END) AS rolledBack,
            SUM(CASE WHEN status = 'Aborted' THEN 1 ELSE 0 END) AS aborted,
            SUM(CASE WHEN status = 'Failed' THEN 1 ELSE 0 END) AS failed,
            AVG(duration_ms) AS meanDurationMs
          FROM deployments
          GROUP BY service
          ORDER BY service
        `.pipe(
          Effect.map((rows) =>
            rows.map((row): ServiceStats => ({
              service: row.service,
              total: row.total,
              succeeded: row.succeeded,
              rolledBack: row.rolledBack,
              aborted: row.aborted,
              failed: row.failed,
              rollbackRate: row.total === 0 ? 0 : row.rolledBack / row.total,
              meanDurationMs: Math.round(row.meanDurationMs)
            }))
          ),
          Effect.orDie
        )

      return { stats }
    })
  )
