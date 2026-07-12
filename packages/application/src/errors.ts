import { Schema } from "effect"

/**
 * Port-contract errors — tech-agnostic failures a port may surface, part of
 * its contract rather than of any concrete backend. Adapters map their own
 * infrastructure failures (e.g. PrometheusUnreachable, NginxReloadFailed)
 * into these so the application layer never names a specific technology.
 *
 * Schema-backed so they cross the Temporal boundary.
 * Business failures (HealthCheckFailed, …) live in @flux/domain instead.
 */

/** The metrics backend could not be reached or returned unusable data. */
export class MetricsUnavailable extends Schema.TaggedErrorClass<MetricsUnavailable>()(
  "MetricsUnavailable",
  {
    service: Schema.String,
    reason: Schema.String
  }
) {}

/** The router backend rejected or failed to apply a traffic-weight change. */
export class RouterUnavailable extends Schema.TaggedErrorClass<RouterUnavailable>()(
  "RouterUnavailable",
  {
    service: Schema.String,
    reason: Schema.String
  }
) {}

/** Delivering a notification failed (non-fatal to a deployment). */
export class NotifyFailed extends Schema.TaggedErrorClass<NotifyFailed>()(
  "NotifyFailed",
  {
    channel: Schema.String,
    reason: Schema.String
  }
) {}
