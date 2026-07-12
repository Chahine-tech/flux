import { Schema } from "effect"
import { ThresholdBreach } from "./thresholds.ts"

/**
 * Domain-level tagged errors — the business-meaningful failures that flow
 * through activities and are part of the ubiquitous language.
 *
 * Built with `Schema.TaggedErrorClass` so they are schema-validated,
 * yieldable in `Effect.gen`, matchable as tagged union members, and
 * serializable across the Temporal boundary.
 *
 * Infrastructure failures (e.g. PrometheusUnreachable, NginxReloadFailed)
 * belong to the adapters, not here.
 */

/** The new version failed its health check before any traffic was shifted. */
export class HealthCheckFailed extends Schema.TaggedErrorClass<HealthCheckFailed>()(
  "HealthCheckFailed",
  {
    service: Schema.String,
    version: Schema.String,
    reason: Schema.String
  }
) {}

/** Observed metrics crossed the failure budget during a canary step. */
export class MetricsThresholdExceeded extends Schema.TaggedErrorClass<MetricsThresholdExceeded>()(
  "MetricsThresholdExceeded",
  {
    service: Schema.String,
    atPercent: Schema.Finite,
    breaches: Schema.NonEmptyArray(ThresholdBreach)
  }
) {}

/** The rollback itself failed — the most severe outcome, needs paging. */
export class RollbackFailed extends Schema.TaggedErrorClass<RollbackFailed>()(
  "RollbackFailed",
  {
    service: Schema.String,
    toVersion: Schema.String,
    reason: Schema.String
  }
) {}
