import { describe, it } from "@effect/vitest"
import { Duration, Schema } from "effect"
import { Arbitrary } from "effect/unstable/arbitrary"
import { expect } from "vitest"
import { DurationFromShorthand } from "../src/duration.ts"
import { evaluateThresholds, type MetricReadings } from "../src/thresholds.ts"

/**
 * Generation is Schema-first since Effect's rc: the core package dropped its
 * dependencies and `it.prop` no longer accepts a fast-check `Arbitrary`, only a
 * `Schema` or one from `effect/unstable/arbitrary`. The bounds below are the
 * same ones the hand-built fast-check generators used, expressed as checks on
 * the schema instead of arguments to a generator.
 */

const decode = Schema.decodeUnknownSync(DurationFromShorthand)
const encode = Schema.encodeUnknownSync(DurationFromShorthand)

describe("evaluateThresholds (property-based)", () => {
  // Each pair is one rule (max) and its observed reading. Finite excludes NaN
  // and the infinities, which is what noNaN/noDefaultInfinity bought before.
  const Reading = Schema.Finite.check(Schema.isBetween({ minimum: 0, maximum: 1000 }))
  const Pairs = Schema.Array(Schema.Struct({ max: Reading, reading: Reading })).check(
    Schema.isMaxLength(8)
  )

  it.prop("breaches exactly the rules whose reading exceeds max", [Pairs], ([samples]) => {
    const rules = samples.map((s, i) => ({ name: `m${i}`, query: `q${i}`, max: s.max }))
    const readings: MetricReadings = Object.fromEntries(samples.map((s, i) => [`m${i}`, { value: s.reading }]))

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
  // The shorthand is a number glued to a unit; generate the two parts under
  // their own constraints and join them, rather than generating from the
  // pattern check on the schema itself.
  const shorthand = Arbitrary.map(
    Arbitrary.schema(
      Schema.Struct({
        n: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 1_000_000 })),
        unit: Schema.Literals(["ms", "s", "m", "h", "d"])
      })
    ),
    ({ n, unit }) => `${n}${unit}`
  )

  it.prop("decodes then re-encodes to the same Duration", [shorthand], ([text]) => {
    const duration = decode(text)
    const roundTripped = decode(encode(duration))
    expect(Duration.toMillis(roundTripped)).toBe(Duration.toMillis(duration))
  })
})
