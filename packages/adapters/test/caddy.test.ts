import { describe, it } from "@effect/vitest"
import { expect } from "vitest"
import { type CaddyRoute, parseRoute, renderHandler, routeId } from "../src/router/caddy.ts"
import { baseline } from "../src/router/weights.ts"

const address = (service: string, version: string) => `${service}-${version}:8080`
const versionOf = (service: string, dial: string) =>
  dial.startsWith(`${service}-`) ? dial.slice(service.length + 1).replace(/:\d+$/, "") : undefined

describe("renderHandler", () => {
  it("renders index-mapped weights for the sorted non-zero versions", () => {
    const handler = renderHandler("api", { "v2": 10, "v1": 90, "v0": 0 }, address) as {
      upstreams: Array<{ dial: string }>
      load_balancing: { selection_policy: { policy: string; weights: Array<number> } }
    }
    expect(handler.upstreams.map((u) => u.dial)).toEqual(["api-v1:8080", "api-v2:8080"])
    expect(handler.load_balancing.selection_policy).toEqual({
      policy: "weighted_round_robin",
      weights: [90, 10]
    })
  })
})

describe("parseRoute", () => {
  it("round-trips what renderHandler wrote, normalized to percentages", () => {
    const route = { handle: [renderHandler("api", { "v1": 90, "v2": 10 }, address)] } as CaddyRoute
    expect(parseRoute(route, "api", versionOf)).toEqual([
      { version: "v1", weight: 90 },
      { version: "v2", weight: 10 }
    ])
  })

  it("treats missing weights as equal (Caddy's own default)", () => {
    const route: CaddyRoute = {
      handle: [{ handler: "reverse_proxy", upstreams: [{ dial: "api-v1:8080" }, { dial: "api-v2:8080" }] }]
    }
    expect(parseRoute(route, "api", versionOf)).toEqual([
      { version: "v1", weight: 50 },
      { version: "v2", weight: 50 }
    ])
  })

  it("labels an upstream it cannot invert by its dial address (visible as drift)", () => {
    const route: CaddyRoute = {
      handle: [{ handler: "reverse_proxy", upstreams: [{ dial: "rogue:9000" }] }]
    }
    expect(parseRoute(route, "api", versionOf)).toEqual([{ version: "rogue:9000", weight: 100 }])
  })

  it("returns empty for a route with no reverse_proxy handler", () => {
    expect(parseRoute({ handle: [{ handler: "static_response" }] } as CaddyRoute, "api", versionOf)).toEqual([])
  })
})

describe("baseline (first-deployment seed)", () => {
  it("seeds the previous version at 100% when the router has no state", () => {
    expect(baseline({}, { version: "v2", previousVersion: "v1" })).toEqual({ "v1": 100 })
  })

  it("leaves known state untouched", () => {
    expect(baseline({ "v1": 90, "v2": 10 }, { version: "v2", previousVersion: "v1" }))
      .toEqual({ "v1": 90, "v2": 10 })
  })

  it("does not seed without a previous version, or when it equals the target", () => {
    expect(baseline({}, { version: "v2" })).toEqual({})
    expect(baseline({}, { version: "v2", previousVersion: "v2" })).toEqual({})
  })
})

describe("routeId", () => {
  it("is stable per service", () => {
    expect(routeId("api")).toBe("flux-api")
  })
})
