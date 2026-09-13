import { Effect, Layer, Tracer } from "effect"
import { afterEach, describe, expect, it } from "vitest"
import { loggerLayer, resource } from "../src/index.ts"

/**
 * Two decisions that would come undone without anyone noticing: a log format
 * that silently drops annotations, and a `serviceVersion` that defaults to
 * something meaningless.
 */
const env = { ...process.env }
afterEach(() => {
  process.env = { ...env }
})

/** What a process actually writes for one annotated line. */
const emit = async (): Promise<string> => {
  const out: Array<string> = []
  const originalLog = console.log
  const originalInfo = console.info
  console.log = (line: string) => void out.push(String(line))
  console.info = (line: string) => void out.push(String(line))
  try {
    await Effect.runPromise(
      Effect.logInfo("canary promoted").pipe(
        Effect.annotateLogs({ workflowId: "dep-api-1", sdkComponent: "workflow" }),
        Effect.provide(loggerLayer())
      )
    )
  } finally {
    console.log = originalLog
    console.info = originalInfo
  }
  return out.join("\n")
}

describe("the log format", () => {
  it("keeps the annotations in JSON", async () => {
    process.env.FLUX_LOG_FORMAT = "json"
    const line = await emit()
    // The annotations are the point. A workflow's `workflowId` is what puts its
    // line beside the activity lines of the same deployment, so a format that
    // dropped them would quietly undo D24's deferred half.
    expect(JSON.parse(line)).toMatchObject({
      message: "canary promoted",
      annotations: { workflowId: "dep-api-1", sdkComponent: "workflow" }
    })
  })

  it("keeps them in the human format too", async () => {
    process.env.FLUX_LOG_FORMAT = "logfmt"
    const line = await emit()
    expect(line).toContain("workflowId=dep-api-1")
    expect(line).toContain("sdkComponent=workflow")
  })

  it("chooses by whether stdout is a terminal, not by NODE_ENV", async () => {
    delete process.env.FLUX_LOG_FORMAT
    const original = process.stdout.isTTY
    const setTTY = (value: boolean) =>
      Object.defineProperty(process.stdout, "isTTY", { value, configurable: true })
    try {
      // A container has no TTY, and so answers the real question ("is a human
      // reading this") correctly without having to be told.
      setTTY(false)
      expect(JSON.parse(await emit())).toMatchObject({ message: "canary promoted" })

      // And an interactive shell gets the readable form, `docker run -it`
      // included, which is the right answer rather than an accident.
      setTTY(true)
      expect(await emit()).toContain("workflowId=dep-api-1")
    } finally {
      Object.defineProperty(process.stdout, "isTTY", { value: original, configurable: true })
    }
  })
})

describe("the span resource", () => {
  it("omits serviceVersion rather than inventing one", () => {
    delete process.env.FLUX_VERSION
    const r = resource("flux-worker")
    // Every package here is 0.0.0. Attaching that to every span would be an
    // answer that distinguishes nothing; absent reads as "unknown", which is
    // the truth.
    expect(r.serviceVersion).toBeUndefined()
    expect(r.serviceName).toBe("flux-worker")
  })

  it("carries it when a build sets one", () => {
    process.env.FLUX_VERSION = "9f3c1ab"
    expect(resource("flux-worker").serviceVersion).toBe("9f3c1ab")
  })

  it("names the environment so one trace store can hold both", () => {
    delete process.env.FLUX_ENV
    expect(resource("flux-cli").attributes["deployment.environment.name"]).toBe("development")
    process.env.FLUX_ENV = "k3d-lab"
    expect(resource("flux-cli").attributes["deployment.environment.name"]).toBe("k3d-lab")
  })
})

describe("how the layer is installed", () => {
  it("still replaces the default logger when merged into a larger layer", async () => {
    // How both apps wire it: `Layer.mergeAll(CoreLayer, TracingLayer,
    // loggerLayer())` in the worker, a `Layer.provide` in the control plane.
    // Whether a logger layer keeps its replacing behaviour through composition
    // is not obvious, and getting it wrong is silent: the format simply stays
    // the default and nobody notices until they grep production logs.
    process.env.FLUX_LOG_FORMAT = "json"
    const out: Array<string> = []
    const original = console.log
    console.log = (line: string) => void out.push(String(line))
    try {
      await Effect.runPromise(
        Effect.logInfo("through a merged layer").pipe(
          Effect.annotateLogs({ workflowId: "dep-api-1" }),
          Effect.provide(Layer.mergeAll(Layer.empty, loggerLayer()))
        )
      )
    } finally {
      console.log = original
    }
    expect(JSON.parse(out.join("\n"))).toMatchObject({
      message: "through a merged layer",
      annotations: { workflowId: "dep-api-1" }
    })
  })
})

describe("the link between a log line and its trace", () => {
  const inSpan = async (format: string): Promise<string> => {
    process.env.FLUX_LOG_FORMAT = format
    const out: Array<string> = []
    const original = console.log
    console.log = (line: string) => void out.push(String(line))
    try {
      await Effect.runPromise(
        Effect.logInfo("monitoring step").pipe(
          Effect.withSpan("flux.monitorStep"),
          Effect.provide(loggerLayer())
        )
      )
    } finally {
      console.log = original
    }
    return out.join("\n")
  }

  it("puts the trace and span ids on the JSON line", async () => {
    const line = JSON.parse(await inSpan("json")) as { readonly traceId?: string; readonly spanId?: string }
    // The direction that matters when you are holding a log line and want the
    // trace it belongs to. It was absent, verified before being built: the
    // `spans` field `formatJson` emits is `withLogSpan`, not this.
    expect(line.traceId).toMatch(/^[0-9a-f]{32}$/)
    expect(line.spanId).toMatch(/^[0-9a-f]{16}$/)
  })

  it("puts them on the human line too", async () => {
    expect(await inSpan("logfmt")).toMatch(/traceId=[0-9a-f]{32} spanId=[0-9a-f]{16}/)
  })

  it("says nothing when there is no span rather than inventing ids", async () => {
    process.env.FLUX_LOG_FORMAT = "json"
    const out: Array<string> = []
    const original = console.log
    console.log = (line: string) => void out.push(String(line))
    try {
      await Effect.runPromise(Effect.logInfo("outside any span").pipe(Effect.provide(loggerLayer())))
    } finally {
      console.log = original
    }
    const line = JSON.parse(out.join("\n")) as Record<string, unknown>
    expect(line.traceId).toBeUndefined()
    expect(line.message).toBe("outside any span")
  })

  it("still records log messages as events on the span", async () => {
    // `Logger.tracerLogger` is in Effect's default set and `Logger.layer`
    // overrides that set, so the first version of this layer silently dropped
    // it. What disappeared was log lines showing up inside their span in
    // Jaeger, which nobody notices, because you only miss it when you go
    // looking.
    const events: Array<string> = []
    const recording = Tracer.make({
      span: (options) => {
        const span = new Tracer.NativeSpan(options)
        const original = span.event.bind(span)
        span.event = (name, time, attributes) => {
          events.push(name)
          original(name, time, attributes)
        }
        return span
      }
    })
    const originalLog = console.log
    console.log = () => {}
    try {
      await Effect.runPromise(
        Effect.logInfo("recorded on the span").pipe(
          Effect.withSpan("flux.monitorStep"),
          Effect.provide(Layer.mergeAll(loggerLayer(), Layer.succeed(Tracer.Tracer, recording)))
        )
      )
    } finally {
      console.log = originalLog
    }
    expect(events).toContain("recorded on the span")
  })
})
