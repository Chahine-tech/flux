import { describe, it } from "@effect/vitest"
import { Cron, Result, Schema } from "effect"
import { FastCheck } from "effect/testing"
import { expect } from "vitest"
import { DeploymentWindow, evaluateWindow } from "../src/window.ts"

const decodeWindow = Schema.decodeUnknownSync(DeploymentWindow)

describe("evaluateWindow (D28)", () => {
  it("an absent window is always open", () => {
    expect(evaluateWindow(undefined, new Date("2026-07-19T03:00:00Z"))._tag).toBe("Open")
  })

  it("opens inside a weekday business-hours window", () => {
    // Sunday 2026-07-19 is a weekend; Monday 2026-07-20 14:00 UTC is inside.
    const window = "* 9-17 * * 1-5"
    expect(evaluateWindow(window, new Date("2026-07-20T14:00:00Z"))._tag).toBe("Open")
  })

  it("closes outside the window and reports the next opening", () => {
    const window = "* 9-17 * * 1-5"
    // Saturday 2026-07-18 — outside; next open is Monday 09:00.
    const decision = evaluateWindow(window, new Date("2026-07-18T14:00:00Z"))
    expect(decision._tag).toBe("Closed")
    if (decision._tag === "Closed") {
      expect(decision.nextAllowed.getTime()).toBeGreaterThan(new Date("2026-07-18T14:00:00Z").getTime())
      // The next allowed instant must itself be inside the window.
      const cron = Result.getOrThrow(Cron.parse(window))
      expect(Cron.match(cron, decision.nextAllowed)).toBe(true)
    }
  })

  it("an unparseable window is treated as open (validation belongs at the schema boundary)", () => {
    expect(evaluateWindow("not a cron", new Date())._tag).toBe("Open")
  })

  it("the schema rejects an invalid cron and accepts a valid one", () => {
    expect(() => decodeWindow("nope")).toThrow()
    expect(decodeWindow("* 9-17 * * 1-5")).toBe("* 9-17 * * 1-5")
  })

  // Property: when Open, `now` genuinely matches the cron; when Closed,
  // `nextAllowed` is strictly in the future and itself matches.
  it("Open ⇔ now matches; Closed ⇒ nextAllowed is a future match", () => {
    const hourRange = FastCheck.integer({ min: 0, max: 23 })
    FastCheck.assert(
      FastCheck.property(
        hourRange,
        hourRange,
        FastCheck.date({ min: new Date("2026-01-01T00:00:00Z"), max: new Date("2026-12-31T23:59:59Z"), noInvalidDate: true }),
        (a, b, now) => {
          const lo = Math.min(a, b)
          const hi = Math.max(a, b)
          const window = `* ${lo}-${hi} * * *`
          const cron = Result.getOrThrow(Cron.parse(window))
          // Mirror the evaluator's minute-flooring (Effect Cron is second-precise).
          const floored = new Date(now)
          floored.setUTCSeconds(0, 0)
          const decision = evaluateWindow(window, now)
          if (decision._tag === "Open") {
            return Cron.match(cron, floored)
          }
          return (
            decision.nextAllowed.getTime() > floored.getTime() &&
            Cron.match(cron, decision.nextAllowed)
          )
        }
      )
    )
  })
})
