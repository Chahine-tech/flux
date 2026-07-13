import { Effect, Layer, ManagedRuntime } from "effect"
import { HealthPort, MetricsPort, NotifyPort, RouterPort } from "@flux/application"
import { describe, expect, it } from "vitest"
import { createActivities } from "../src/activities/deployment.activities.ts"

const runtime = ManagedRuntime.make(
  Layer.mergeAll(
    Layer.succeed(HealthPort, { check: () => Effect.void }),
    Layer.succeed(MetricsPort, { query: () => Effect.succeed(0) }),
    Layer.succeed(RouterPort, { setTrafficWeight: () => Effect.void, readState: () => Effect.succeed([]) }),
    Layer.succeed(NotifyPort, { send: () => Effect.void })
  )
)
const acts = createActivities(runtime)

describe("activity input validation (Schema at the Effect boundary)", () => {
  it("accepts a well-formed payload", async () => {
    await expect(acts.setTrafficWeight({ service: "api", version: "v2", weight: 50 })).resolves.toBeUndefined()
  })

  it("rejects an out-of-range weight (type-valid but Schema-invalid)", async () => {
    await expect(acts.setTrafficWeight({ service: "api", version: "v2", weight: 150 })).rejects.toThrow()
  })

  it("rejects an empty service name", async () => {
    await expect(acts.setTrafficWeight({ service: "", version: "v2", weight: 50 })).rejects.toThrow()
  })

  it("rejects monitorStep with an empty rules list", async () => {
    await expect(
      acts.monitorStep({ service: "api", version: "v2", windowMs: 0, pollIntervalMs: 100, rules: [] })
    ).rejects.toThrow()
  })
})
