import { Effect } from "effect"
import { afterEach, describe, expect, it } from "vitest"
import { loggerLayer } from "../src/index.ts"

/**
 * The terminal span tree. What it has to get right is not the drawing but the
 * placement: a log line belongs under the span that emitted it, and children
 * belong under their parent in the order they started.
 */
const env = { ...process.env }
afterEach(() => {
  process.env = { ...env }
})

const capture = async (program: Effect.Effect<void>): Promise<string> => {
  process.env.FLUX_TRACE_CONSOLE = "1"
  const out: Array<string> = []
  const original = console.log
  console.log = (line: string) => void out.push(String(line))
  try {
    await Effect.runPromise(program.pipe(Effect.provide(loggerLayer())))
  } finally {
    console.log = original
  }
  // Strip colours so the assertions are about structure, not presentation.
  return out.join("\n").replace(/\[[0-9;]*m/g, "")
}

describe("the span tree", () => {
  it("nests children under their parent and logs under the span that emitted them", async () => {
    const rendered = await capture(
      Effect.gen(function*() {
        yield* Effect.logInfo("shifting traffic").pipe(Effect.annotateLogs({ percent: 10 }))
        yield* Effect.void.pipe(Effect.withSpan("http.client GET"))
        yield* Effect.void.pipe(Effect.withSpan("http.client PATCH"))
      }).pipe(Effect.withSpan("flux.shiftTraffic"))
    )
    const lines = rendered.split("\n").filter((line) => line.trim() !== "")

    expect(lines[0]).toContain("flux.shiftTraffic")
    // Everything below the root is indented under it, in the order it happened,
    // which is the only reason to draw this rather than print lines.
    expect(lines[1]).toMatch(/^[├└]─ ● INFO shifting traffic \{ percent=10 \}$/)
    expect(lines[2]).toMatch(/^[├└]─ ◇ . http\.client GET\s+✓/)
    expect(lines[3]).toMatch(/^└─ ◇ . http\.client PATCH\s+✓/)
  })

  it("prints one tree per local root, once that root ends", async () => {
    const rendered = await capture(
      Effect.gen(function*() {
        yield* Effect.void.pipe(Effect.withSpan("first"))
        yield* Effect.void.pipe(Effect.withSpan("second"))
      })
    )
    // Two roots, so two trees, rather than one tree with two children.
    expect(rendered.split("\n").filter((line) => line.startsWith("◆")).length).toBe(2)
  })

  it("still prints a log line that belongs to no span", async () => {
    // Losing output to make the picture tidier would be the wrong trade.
    expect(await capture(Effect.logInfo("outside any span"))).toContain("outside any span")
  })

  it("says how long each span took", async () => {
    expect(await capture(Effect.sleep("15 millis").pipe(Effect.withSpan("slow")))).toMatch(/slow\s+✓ \d+(\.\d)?ms/)
  })

  it("interleaves a log line between the spans it was emitted between", async () => {
    // The first version ordered by timestamp and mixed two clocks: spans carry
    // monotonic nanoseconds, a log line only carries a `Date`. The symptom was
    // a warning drawn after both calls it sat between. Ordering by the sequence
    // events actually arrived in is exact, and this view only ever draws one
    // process, which is where that is true.
    const rendered = await capture(
      Effect.gen(function*() {
        yield* Effect.sleep("5 millis").pipe(Effect.withSpan("before"))
        yield* Effect.logWarning("in between")
        yield* Effect.sleep("5 millis").pipe(Effect.withSpan("after"))
      }).pipe(Effect.withSpan("root"))
    )
    const order = rendered.split("\n").filter((line) => line.trim() !== "").slice(1)
    expect(order[0]).toContain("before")
    expect(order[1]).toContain("in between")
    expect(order[2]).toContain("after")
  })

  it("marks a span that failed as failed", async () => {
    // Ending and succeeding are not the same thing, and the first version only
    // looked at whether the span had ended, so an activity that had just thrown
    // was drawn with a green tick.
    const rendered = await capture(
      Effect.fail("boom").pipe(Effect.withSpan("flux.healthCheck"), Effect.ignore)
    )
    expect(rendered).toMatch(/flux\.healthCheck\s+✗/)
    expect(rendered).not.toMatch(/flux\.healthCheck\s+✓/)
  })

  it("leaves the line logger alone when it is not asked for", async () => {
    // The two are exclusive on purpose: the tree already contains the log
    // lines, so running both would print everything twice.
    process.env.FLUX_TRACE_CONSOLE = "0"
    process.env.FLUX_LOG_FORMAT = "json"
    const out: Array<string> = []
    const original = console.log
    console.log = (line: string) => void out.push(String(line))
    try {
      await Effect.runPromise(
        Effect.logInfo("a plain line").pipe(Effect.withSpan("some.span"), Effect.provide(loggerLayer()))
      )
    } finally {
      console.log = original
    }
    expect(JSON.parse(out.join("\n"))).toMatchObject({ message: "a plain line" })
  })
})
