import { Effect, Layer, ManagedRuntime, Stream } from "effect"
import { AiError, LanguageModel } from "effect/unstable/ai"
import { ChangelogPort, ChangelogUnavailable, HealthPort, MetricsPort, NotifyPort, RouterPort } from "@flux/application"
import { describe, expect, it } from "vitest"
import { createActivities } from "../src/activities/deployment.activities.ts"

// A disabled LanguageModel: every request fails. The postmortem activity is
// best-effort, so this exercises its swallow-and-log path without a real LLM.
const disabledError = new AiError.AiError({
  module: "test",
  method: "generateText",
  reason: new AiError.InternalProviderError({ description: "disabled in test" })
})
const disabledModel = Layer.effect(
  LanguageModel.LanguageModel,
  LanguageModel.make({
    generateText: () => Effect.fail(disabledError),
    streamText: () => Stream.fail(disabledError)
  })
)

const runtime = ManagedRuntime.make(
  Layer.mergeAll(
    Layer.succeed(HealthPort, { check: () => Effect.void }),
    Layer.succeed(MetricsPort, { query: () => Effect.succeed(0) }),
    Layer.succeed(RouterPort, { setTrafficWeight: () => Effect.void, readState: () => Effect.succeed([]) }),
    Layer.succeed(NotifyPort, { send: () => Effect.void }),
    disabledModel,
    Layer.succeed(ChangelogPort, {
      between: ({ service }) => Effect.fail(new ChangelogUnavailable({ service, reason: "disabled in test" }))
    })
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

  // The postmortem is best-effort by construction: even when the model
  // fails, the activity resolves — a rollback that already happened must never
  // be undone by a failed postmortem.
  it("postmortem never rejects, even when the model is unavailable", async () => {
    await expect(
      acts.postmortem({
        service: "api",
        version: "v2",
        previousVersion: "v1",
        atPercent: 50,
        breaches: [{ metric: "error_rate", observed: 0.08, limit: 0.01 }]
      })
    ).resolves.toBeUndefined()
  })
})
