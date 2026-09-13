import { Effect, Layer, Logger, ManagedRuntime, References } from "effect"
import { describe, expect, it } from "vitest"
import { effectLogger } from "../src/temporal-logger.ts"

/**
 * The half of D24 that was deferred: a workflow's own log lines reaching
 * Effect's logger instead of going out to stderr on their own.
 *
 * What matters is not that the message arrives, it is that its *metadata*
 * arrives as annotations. A workflow log carries `workflowId`, `runId`,
 * `workflowType` and `sdkComponent`, and those are what put it beside the
 * activity logs of the same deployment rather than in a stream of its own,
 * which was the whole complaint.
 */
const captured: Array<{ readonly message: unknown; readonly level: string; readonly annotations: unknown }> = []

const capturingLogger = Logger.make((options) => {
  captured.push({
    message: options.message,
    level: options.logLevel,
    // Annotations live on the fiber, as a plain record, which is how a real
    // logger reads them.
    annotations: options.fiber.getRef(References.CurrentLogAnnotations)
  })
})

const runtime = ManagedRuntime.make(
  Layer.mergeAll(
    Logger.layer([capturingLogger]),
    Layer.succeed(References.MinimumLogLevel, "Trace" as never)
  )
)

describe("Temporal logs through Effect", () => {
  it("carries the workflow's metadata as log annotations", () => {
    captured.length = 0
    const logger = effectLogger(runtime)

    logger.info("canary promoted", {
      workflowId: "dep-api-1",
      runId: "abc",
      workflowType: "deploymentWorkflow",
      sdkComponent: "workflow"
    })

    expect(captured).toHaveLength(1)
    const entry = captured[0]!
    expect(entry.message).toContain("canary promoted")
    expect(entry.annotations).toMatchObject({
      workflowId: "dep-api-1",
      workflowType: "deploymentWorkflow",
      sdkComponent: "workflow"
    })
  })

  it("maps every Temporal level onto Effect's", () => {
    captured.length = 0
    const logger = effectLogger(runtime)
    logger.trace("t")
    logger.debug("d")
    logger.info("i")
    logger.warn("w")
    logger.error("e")
    // Effect's own level name for a warning is "Warn".
    expect(captured.map((c) => String(c.level))).toEqual(["Trace", "Debug", "Info", "Warn", "Error"])
  })

  it("never lets a logging failure reach the worker", () => {
    // A logger that can take the worker down is worse than a lost line, so the
    // forward swallows its own failures. Proven with a runtime already closed.
    const dead = ManagedRuntime.make(Layer.empty)
    void dead.dispose()
    expect(() => effectLogger(dead).error("boom", { workflowId: "x" })).not.toThrow()
  })
})
