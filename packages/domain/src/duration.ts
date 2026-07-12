import { Duration, Schema, SchemaGetter } from "effect"

/**
 * Shorthand duration format used across flux config (`"5m"`, `"30s"`, `"1h"`).
 *
 * Effect's own `Schema.DurationFromString` only accepts the long form
 * (`"5 minutes"`), so we build a custom bidirectional transform:
 *
 *   string (shorthand) ──decode──▶ Duration ──encode──▶ string (shorthand)
 *
 * The input string is first constrained by a pattern check, so the decode
 * getter can parse it as a pure, total function (no failure path needed).
 */

const UNIT_TO_MILLIS = {
  ms: 1,
  s: 1_000,
  m: 60_000,
  h: 3_600_000,
  d: 86_400_000
} as const

type Unit = keyof typeof UNIT_TO_MILLIS

// Largest unit first, so encoding picks the most readable representation.
const UNITS_DESC: ReadonlyArray<Unit> = ["d", "h", "m", "s", "ms"]

const SHORTHAND_PATTERN = /^(\d+(?:\.\d+)?)\s*(ms|s|m|h|d)$/

/** Parse a pre-validated shorthand string into a Duration. */
const parseShorthand = (input: string): Duration.Duration => {
  const match = SHORTHAND_PATTERN.exec(input.trim())
  // Guaranteed by the pattern check on the source schema.
  if (match === null) {
    throw new Error(`unreachable: invalid duration shorthand "${input}"`)
  }
  const value = Number(match[1])
  const unit = match[2] as Unit
  return Duration.millis(value * UNIT_TO_MILLIS[unit])
}

/** Render a Duration back to the largest whole shorthand unit (falls back to ms). */
const formatShorthand = (duration: Duration.Duration): string => {
  const millis = Duration.toMillis(duration)
  for (const unit of UNITS_DESC) {
    const factor = UNIT_TO_MILLIS[unit]
    if (millis % factor === 0) {
      return `${millis / factor}${unit}`
    }
  }
  return `${millis}ms`
}

/**
 * A `Duration` encoded as a shorthand string (`"5m"`).
 * Decoded type: `Duration`. Encoded type: `string`.
 */
export const DurationFromShorthand = Schema.String.pipe(
  Schema.check(Schema.isPattern(SHORTHAND_PATTERN)),
  Schema.decodeTo(Schema.Duration, {
    decode: SchemaGetter.transform(parseShorthand),
    encode: SchemaGetter.transform(formatShorthand)
  })
)

export type DurationFromShorthand = typeof DurationFromShorthand.Type
