import { Effect, Option } from "effect"
import { describe, expect, it } from "vitest"
import { tracedCommand } from "../src/tracing.ts"

/**
 * The root span `tracedCommand` adds is the whole of the CLI's side of the
 * trace hop, so the one thing worth pinning is that the span exists whether or
 * not a collector is configured.
 *
 * That is not obvious from the code: the OTLP layer is gated on
 * `OTLP_ENDPOINT`, and the easy mistake would be to gate the span with it. The
 * span has to be unconditional, because `HttpClient` derives the outgoing
 * `traceparent` from it and the native tracer generates real ids just fine. A
 * CLI with no collector still propagates a trace the control plane can
 * continue; it just does not record its own half.
 */
const spanName = Effect.map(Effect.option(Effect.currentSpan), Option.map((span) => span.name))

describe("flux CLI root span", () => {
  it("wraps the command when no collector is configured", async () => {
    const previous = process.env
    process.env = { ...previous }
    delete process.env.OTLP_ENDPOINT
    try {
      const name = await Effect.runPromise(spanName.pipe(tracedCommand("deploy")))
      expect(name).toEqual(Option.some("flux deploy"))
    } finally {
      process.env = previous
    }
  })

  // The configured case is deliberately not exercised here. It would spend the
  // exporter's shutdown timeout failing to reach a collector, and the span it
  // would assert is the same one, created by the same `Effect.withSpan`. What
  // the OTLP layer adds is proven where it can be observed instead:
  // `apps/control-plane/test/tracing.test.ts` runs the real layer against a
  // stub collector and reads the exported span off the wire.
})
