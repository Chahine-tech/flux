import { describe, it } from "@effect/vitest"
import { expect } from "vitest"
import { compareMeanToLimit, compareToLimit, meanInterval, tCritical, wilsonInterval, Z_95 } from "../src/confidence.ts"
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

describe("meanInterval", () => {
  it("says nothing from a single measurement", () => {
    // One observation has no spread to estimate, so it supports no statement
    // about the next one. Unbounded is the honest answer.
    const { lower, upper } = meanInterval(0.1, 0, 1)
    expect(lower).toBe(Number.NEGATIVE_INFINITY)
    expect(upper).toBe(Number.POSITIVE_INFINITY)
  })

  it("uses Student, so small samples get wider intervals than z would give", () => {
    // With sigma estimated from the sample, `z` makes the interval too narrow
    // and concludes too early, which is the failure this is here to prevent.
    const small = meanInterval(0.1, 0.05, 5)
    const zWidth = 2 * 1.96 * (0.05 / Math.sqrt(5))
    expect(small.upper - small.lower).toBeGreaterThan(zWidth)
  })

  it("converges on z as the sample grows", () => {
    expect(tCritical(5)).toBeCloseTo(2.571, 3)
    expect(tCritical(60)).toBeCloseTo(2.0, 2)
    expect(tCritical(100_000)).toBeCloseTo(Z_95, 3)
  })
})

describe("compareMeanToLimit, the cost regression case", () => {
  const limit = 0.10

  it("calls a 38% cost increase what it is, given enough tasks", () => {
    // $0.138 per task against a $0.10 budget, over 40 tasks: the whole interval
    // sits above the limit, so the regression is real and not sampling noise.
    expect(compareMeanToLimit(0.138, 0.05, 40, limit)).toBe("above")
  })

  it("will not call it on five tasks", () => {
    // Same mean, same spread, eight times less evidence.
    expect(compareMeanToLimit(0.138, 0.05, 5, limit)).toBe("unknown")
  })

  it("stays undecided on a small increase", () => {
    // $0.104 is over budget on the nose and inside the noise.
    expect(compareMeanToLimit(0.104, 0.05, 40, limit)).toBe("unknown")
  })

  it("clears a version that is genuinely cheaper", () => {
    expect(compareMeanToLimit(0.06, 0.02, 40, limit)).toBe("below")
  })
})

describe("what a breach calls for", () => {
  const costRule: Thresholds = [
    { name: "costPerTask", query: "q", max: 0.10, sampleSize: "n", stdDev: "s", onBreach: "pause" }
  ]

  it("asks to pause when only pause rules breached", () => {
    const result = evaluateThresholds({ costPerTask: { value: 0.138, sampleSize: 40, stdDev: 0.05 } }, costRule)
    expect(result._tag).toBe("Breached")
    if (result._tag === "Breached") expect(result.action).toBe("pause")
  })

  it("rolls back when a fault breached alongside a tradeoff", () => {
    // A genuine technical regression is not up for discussion, whatever the
    // cost rule wanted.
    const both: Thresholds = [
      ...costRule,
      { name: "errorRate", query: "q2", max: 0.05, sampleSize: "n2" }
    ]
    const result = evaluateThresholds({
      costPerTask: { value: 0.138, sampleSize: 40, stdDev: 0.05 },
      errorRate: { value: 0.4, sampleSize: 500 }
    }, both)
    expect(result._tag).toBe("Breached")
    if (result._tag === "Breached") expect(result.action).toBe("rollback")
  })

  it("defaults to rolling back when the rule says nothing", () => {
    const plain: Thresholds = [{ name: "errorRate", query: "q", max: 0.05 }]
    const result = evaluateThresholds({ errorRate: { value: 0.4 } }, plain)
    if (result._tag === "Breached") expect(result.action).toBe("rollback")
  })

  it("omits the bounds it cannot compute rather than serialising an infinity", () => {
    // These cross Temporal as JSON, where Infinity becomes null and the field
    // would come back wrong instead of missing.
    const result = evaluateThresholds({ costPerTask: { value: 0.2, sampleSize: 1, stdDev: 0.05 } }, costRule)
    expect(result._tag).toBe("Inconclusive")
    if (result._tag === "Inconclusive") {
      expect(result.pending[0].lower).toBeUndefined()
      expect(result.pending[0].upper).toBeUndefined()
      expect(JSON.parse(JSON.stringify(result)).pending[0]).not.toHaveProperty("lower")
    }
  })
})
