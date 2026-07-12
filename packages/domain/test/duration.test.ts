import { describe, it } from "@effect/vitest"
import { Duration, Schema } from "effect"
import { expect } from "vitest"
import { DurationFromShorthand } from "../src/duration.ts"

const decode = Schema.decodeUnknownSync(DurationFromShorthand)
const encode = Schema.encodeUnknownSync(DurationFromShorthand)

describe("DurationFromShorthand", () => {
  it("decodes each supported unit", () => {
    expect(Duration.toMillis(decode("500ms"))).toBe(500)
    expect(Duration.toMillis(decode("30s"))).toBe(30_000)
    expect(Duration.toMillis(decode("5m"))).toBe(300_000)
    expect(Duration.toMillis(decode("2h"))).toBe(7_200_000)
    expect(Duration.toMillis(decode("1d"))).toBe(86_400_000)
  })

  it("tolerates whitespace and decimals", () => {
    expect(Duration.toMillis(decode("10 m"))).toBe(600_000)
    expect(Duration.toMillis(decode("1.5h"))).toBe(5_400_000)
  })

  it("rejects the long Effect form and garbage", () => {
    expect(() => decode("5 minutes")).toThrow()
    expect(() => decode("soon")).toThrow()
    expect(() => decode("")).toThrow()
  })

  it("encodes back to the largest whole unit", () => {
    expect(encode(Duration.minutes(5))).toBe("5m")
    expect(encode(Duration.seconds(90))).toBe("90s")
    expect(encode(Duration.hours(2))).toBe("2h")
  })
})
