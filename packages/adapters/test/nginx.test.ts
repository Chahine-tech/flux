import { describe, it } from "@effect/vitest"
import { expect } from "vitest"
import { redistribute, renderUpstreams, type RouterState } from "../src/router/nginx.ts"

describe("redistribute (two-version canary model)", () => {
  it("gives the complement to the single other version", () => {
    // previous was live at 100%; route 10% to the new version.
    const result = redistribute({ "v2.0.8": 100 }, "v2.1.0", 10)
    expect(result["v2.1.0"]).toBe(10)
    expect(result["v2.0.8"]).toBe(90)
  })

  it("seeds the only version when the registry is empty", () => {
    expect(redistribute({}, "v2.0.8", 100)).toEqual({ "v2.0.8": 100 })
  })

  it("splits the remainder evenly when other weights are all zero", () => {
    const result = redistribute({ a: 0, b: 0 }, "c", 50)
    expect(result.c).toBe(50)
    expect(result.a).toBe(25)
    expect(result.b).toBe(25)
  })

  it("full rollout drives the previous version to zero", () => {
    const result = redistribute({ "v2.0.8": 90, "v2.1.0": 10 }, "v2.1.0", 100)
    expect(result["v2.1.0"]).toBe(100)
    expect(result["v2.0.8"]).toBe(0)
  })
})

describe("renderUpstreams", () => {
  const address = (service: string, version: string) => `${service}-${version}:8080`

  it("renders a weighted upstream block, omitting zero-weight servers", () => {
    const state: RouterState = { api: { "v2.0.8": 90, "v2.1.0": 10, "v1.9.0": 0 } }
    const config = renderUpstreams(state, { address })
    expect(config).toContain("upstream api {")
    expect(config).toContain("server api-v2.0.8:8080 weight=90;")
    expect(config).toContain("server api-v2.1.0:8080 weight=10;")
    expect(config).not.toContain("v1.9.0")
  })

  it("rounds fractional weights to integers for nginx", () => {
    const state: RouterState = { api: { a: 33.3, b: 66.7 } }
    const config = renderUpstreams(state, { address })
    expect(config).toContain("weight=33;")
    expect(config).toContain("weight=67;")
  })
})
