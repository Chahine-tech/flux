import { Effect, Logger, References } from "effect"
import { describe, expect, it } from "vitest"
import { withDeploymentLog } from "../src/activities/deployment.activities.ts"

/**
 * D29: `withDeploymentLog` (used by every activity via `linkToDeployment`)
 * stamps the deployment's business id onto every log line the use case emits,
 * through `Effect.annotateLogs` — v4's successor to FiberRef. Proven with a
 * capturing logger that reads the fiber's current log annotations.
 */

const captureAnnotations = async <A, E>(effect: Effect.Effect<A, E>): Promise<Array<Record<string, unknown>>> => {
  const captured: Array<Record<string, unknown>> = []
  const capturing = Logger.make((options) => {
    captured.push(options.fiber.getRef(References.CurrentLogAnnotations))
  })
  await Effect.runPromise(effect.pipe(Effect.provide(Logger.layer([capturing]))))
  return captured
}

describe("deployment log correlation (D29)", () => {
  it("stamps flux.deployment onto a log emitted inside the wrapper", async () => {
    const annotations = await captureAnnotations(withDeploymentLog("dep-api-123", Effect.log("shifting traffic")))
    expect(annotations).toHaveLength(1)
    expect(annotations[0]).toMatchObject({ "flux.deployment": "dep-api-123" })
  })

  it("is a no-op when there is no deployment id (unit-test context)", async () => {
    const annotations = await captureAnnotations(withDeploymentLog(undefined, Effect.log("no context")))
    expect(annotations).toHaveLength(1)
    expect(annotations[0]).not.toHaveProperty("flux.deployment")
  })
})
