import { describe, it } from "@effect/vitest"
import { expect } from "vitest"
import { compareToLimit, wilsonInterval } from "../src/confidence.ts"
import type { Thresholds } from "../src/config.ts"
import { evaluateThresholds } from "../src/thresholds.ts"

describe("wilsonInterval", () => {
  it("says nothing when it has seen nothing", () => {
    // Not an edge case to tidy away: no observations support no conclusion, and
    // the whole range is the honest answer.
    expect(wilsonInterval(0, 0)).toEqual({ lower: 0, upper: 1 })
  })

  it("keeps its width when the observed count is zero", () => {
    // The textbook interval collapses to [0, 0] here and announces certainty
    // exactly where there is none. Twenty tries is not enough to have met a 5%
    // event, and the interval has to say so.
    const { lower, upper } = wilsonInterval(0, 20)
    expect(lower).toBe(0)
    expect(upper).toBeCloseTo(0.161, 2)
  })

  it("stays inside the unit range", () => {
    for (const [k, n] of [[0, 1], [1, 1], [1, 3], [7, 7], [1, 1000]] as const) {
      const { lower, upper } = wilsonInterval(k, n)
      expect(lower).toBeGreaterThanOrEqual(0)
      expect(upper).toBeLessThanOrEqual(1)
      expect(lower).toBeLessThanOrEqual(upper)
    }
  })

  it("narrows as observations accumulate", () => {
    const width = (n: number): number => {
      const { lower, upper } = wilsonInterval(0.05 * n, n)
      return upper - lower
    }
    expect(width(20)).toBeGreaterThan(width(200))
    expect(width(200)).toBeGreaterThan(width(2000))
  })
})

describe("compareToLimit", () => {
  it("cannot clear a limit on a handful of observations", () => {
    // 1 failure in 30 reads as 3.3% against a 5% limit, which a bare comparison
    // calls healthy. The true rate consistent with that sample runs to 16.7%.
    expect(compareToLimit(1 / 30, 30, 0.05)).toBe("unknown")
  })

  it("cannot clear a limit on a perfect but tiny sample", () => {
    // The worst case is the one that looks best.
    expect(compareToLimit(0, 20, 0.05)).toBe("unknown")
  })

  it("clears the limit once there are enough observations", () => {
    expect(compareToLimit(0, 200, 0.05)).toBe("below")
  })

  it("is still undecided when the rate sits on the limit, even at n=1000", () => {
    // 50 in 1000 is exactly 5% against a 5% limit: the interval straddles it,
    // and more data is the only thing that settles a tie this close.
    expect(compareToLimit(0.05, 1000, 0.05)).toBe("unknown")
  })

  it("calls a real breach a breach", () => {
    expect(compareToLimit(0.2, 1000, 0.05)).toBe("above")
    expect(compareToLimit(0.5, 30, 0.05)).toBe("above")
  })
})

describe("evaluateThresholds with sample sizes", () => {
  const rule: Thresholds = [{ name: "taskFailureRate", query: "q", max: 0.05, sampleSize: "n" }]

  it("refuses to promote on a sample that cannot decide", () => {
    const result = evaluateThresholds({ taskFailureRate: { value: 1 / 30, sampleSize: 30 } }, rule)
    expect(result._tag).toBe("Inconclusive")
    if (result._tag === "Inconclusive") {
      expect(result.pending[0].metric).toBe("taskFailureRate")
      expect(result.pending[0].sampleSize).toBe(30)
      expect(result.pending[0].upper).toBeGreaterThan(0.05)
    }
  })

  it("is Within once the evidence supports it", () => {
    expect(evaluateThresholds({ taskFailureRate: { value: 0, sampleSize: 200 } }, rule)._tag).toBe("Within")
  })

  it("breaches when the whole interval clears the limit", () => {
    expect(evaluateThresholds({ taskFailureRate: { value: 0.2, sampleSize: 500 } }, rule)._tag).toBe("Breached")
  })

  it("keeps the bare comparison when no sample size was read", () => {
    // The historical regime, and still the right one for a metric backed by
    // thousands of requests.
    expect(evaluateThresholds({ taskFailureRate: { value: 0.033 } }, rule)._tag).toBe("Within")
  })

  it("keeps the bare comparison for a value that is not a proportion", () => {
    // A latency handed a sample size is a configuration mistake. Falling back
    // beats inventing a confident number for it.
    const latency: Thresholds = [{ name: "p99", query: "q", max: 500, sampleSize: "n" }]
    expect(evaluateThresholds({ p99: { value: 612, sampleSize: 40 } }, latency)._tag).toBe("Breached")
    expect(evaluateThresholds({ p99: { value: 300, sampleSize: 40 } }, latency)._tag).toBe("Within")
  })

  it("lets a confirmed breach outrank an undecided rule", () => {
    const two: Thresholds = [
      { name: "certain", query: "q1", max: 0.05, sampleSize: "n1" },
      { name: "undecided", query: "q2", max: 0.05, sampleSize: "n2" }
    ]
    const result = evaluateThresholds({
      certain: { value: 0.4, sampleSize: 500 },
      undecided: { value: 0, sampleSize: 10 }
    }, two)
    expect(result._tag).toBe("Breached")
  })
})
