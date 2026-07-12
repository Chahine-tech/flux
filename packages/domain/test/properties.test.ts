import { describe, it } from "@effect/vitest"
import { Duration, Schema } from "effect"
import { FastCheck } from "effect/testing"
import { expect } from "vitest"
import { DurationFromShorthand } from "../src/duration.ts"
import { evaluateThresholds, type MetricReadings } from "../src/thresholds.ts"

const decode = Schema.decodeUnknownSync(DurationFromShorthand)
const encode = Schema.encodeUnknownSync(DurationFromShorthand)

describe("evaluateThresholds (property-based)", () => {
  // Each pair is one rule (max) and its observed reading.
  const pairs = FastCheck.array(
    FastCheck.record({
      max: FastCheck.double({ min: 0, max: 1000, noNaN: true, noDefaultInfinity: true }),
      reading: FastCheck.double({ min: 0, max: 1000, noNaN: true, noDefaultInfinity: true })
    }),
    { maxLength: 8 }
  )

  it.prop("breaches exactly the rules whose reading exceeds max", [pairs], ([samples]) => {
    const rules = samples.map((s, i) => ({ name: `m${i}`, query: `q${i}`, max: s.max }))
    const readings: MetricReadings = Object.fromEntries(samples.map((s, i) => [`m${i}`, s.reading]))

    const result = evaluateThresholds(readings, rules)
    const expected = samples.filter((s) => s.reading > s.max).length

    if (expected === 0) {
      expect(result._tag).toBe("Within")
    } else {
      expect(result._tag).toBe("Breached")
      if (result._tag === "Breached") {
        expect(result.breaches).toHaveLength(expected)
      }
    }
  })
})

describe("DurationFromShorthand (property-based)", () => {
  const shorthand = FastCheck.tuple(
    FastCheck.integer({ min: 0, max: 1_000_000 }),
    FastCheck.constantFrom("ms", "s", "m", "h", "d")
  ).map(([n, unit]) => `${n}${unit}`)

  it.prop("decodes then re-encodes to the same Duration", [shorthand], ([text]) => {
    const duration = decode(text)
    const roundTripped = decode(encode(duration))
    expect(Duration.toMillis(roundTripped)).toBe(Duration.toMillis(duration))
  })
})
