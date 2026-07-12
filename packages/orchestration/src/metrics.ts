import { Effect, Metric } from "effect"

/**
 * flux self-instrumentation — the meta loop: flux exposes these on `/metrics`
 * and the same Prometheus it scrapes for deployment health also scrapes flux
 * itself. Counters are updated at the activity boundary (Effect side).
 */

export const deploymentsTotal = Metric.counter("flux_deployments_total", {
  description: "Total flux deployments by terminal outcome"
})

export const trafficShiftsTotal = Metric.counter("flux_traffic_shifts_total", {
  description: "Total traffic-weight changes applied"
})

/** Increment the deployment counter for a terminal outcome (Succeeded/RolledBack/…). */
export const recordOutcome = (outcome: string): Effect.Effect<void> =>
  Metric.update(Metric.withAttributes(deploymentsTotal, { outcome }), 1)

/** Count one applied traffic-weight change. */
export const recordTrafficShift: Effect.Effect<void> = Metric.update(trafficShiftsTotal, 1)

// --- Prometheus text exposition (format built by hand, no exporter dep) ---

const promType = (type: Metric.Metric.Snapshot["type"]): string =>
  type === "Counter" ? "counter" : type === "Gauge" ? "gauge" : "untyped"

const readValue = (snapshot: Metric.Metric.Snapshot): number | bigint | undefined => {
  switch (snapshot.type) {
    case "Counter":
      return snapshot.state.count
    case "Gauge":
      return snapshot.state.value
    default:
      return undefined
  }
}

const formatLabels = (attributes: Metric.Metric.AttributeSet | undefined): string => {
  const entries = Object.entries(attributes ?? {})
  return entries.length === 0
    ? ""
    : `{${entries.map(([key, value]) => `${key}="${value}"`).join(",")}}`
}

/** Render the current metric registry as Prometheus text exposition format. */
export const metricsPrometheusText: Effect.Effect<string> = Metric.snapshot.pipe(
  Effect.map((snapshots) => {
    // Group by metric name so HELP/TYPE are emitted once, before their samples.
    const families = new Map<string, { description?: string; type: string; samples: string[] }>()
    for (const snapshot of snapshots) {
      const value = readValue(snapshot)
      if (value === undefined) {
        continue
      }
      let family = families.get(snapshot.id)
      if (family === undefined) {
        family = { type: promType(snapshot.type), samples: [], ...(snapshot.description !== undefined ? { description: snapshot.description } : {}) }
        families.set(snapshot.id, family)
      }
      family.samples.push(`${snapshot.id}${formatLabels(snapshot.attributes)} ${value}`)
    }

    const lines: string[] = []
    for (const [id, family] of families) {
      if (family.description !== undefined) {
        lines.push(`# HELP ${id} ${family.description}`)
      }
      lines.push(`# TYPE ${id} ${family.type}`)
      lines.push(...family.samples)
    }
    return `${lines.join("\n")}\n`
  })
)
