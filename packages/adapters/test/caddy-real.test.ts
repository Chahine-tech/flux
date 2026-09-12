import { Effect, Layer } from "effect"
import { NodeHttpClient } from "@effect/platform-node"
import { RouterPort } from "@flux/application"
import { beforeEach, describe, expect, it } from "vitest"
import * as CaddyRouter from "../src/router/caddy.ts"

/**
 * The Caddy adapter against a **real Caddy**, not the HTTP double the rest of
 * the suite uses.
 *
 * D20 built this adapter as the deliberate opposite of nginx's shape and proved
 * the port by the absence of change above it. What it never did is talk to
 * Caddy: the unit tests cover the pure render/parse functions and the
 * integration test drives an admin-API double. That gap matters now, because
 * D40 concluded this is the only adapter whose shape fits a pod topology —
 * nginx writes a file and signals a process, so it assumes co-location — which
 * would leave a whole tier resting on an adapter that had only ever spoken to a
 * fake.
 *
 * Gated on a running Caddy:
 *   docker compose --profile demo up -d caddy demo-target
 *   FLUX_REAL_CADDY=1 pnpm --filter @flux/adapters test
 *
 * `demo-target` answers for `api-v1`, `api-v2` and `api-v3` through compose
 * network aliases, so the dial addresses this writes actually resolve and the
 * route Caddy ends up with is one it can serve rather than one it merely
 * stores.
 */
const REAL = process.env.FLUX_REAL_CADDY === "1"
const ADMIN = process.env.CADDY_ADMIN_URL ?? "http://localhost:2019"
const PROXY = process.env.CADDY_PROXY_URL ?? "http://localhost:8090"
const SERVICE = "api"

/** whoami listens on 80; the aliases resolve inside the compose network. */
const address = (service: string, version: string) => `${service}-${version}:80`
const versionOf = (service: string, dial: string) =>
  dial.startsWith(`${service}-`) ? dial.slice(service.length + 1).replace(/:\d+$/, "") : undefined

const layer = CaddyRouter.layer({ adminUrl: ADMIN, server: "flux", address, versionOf }).pipe(
  Layer.provide(NodeHttpClient.layerUndici)
)

/**
 * `previousVersion` is what the workflow passes, and it matters on the very
 * first shift: with nothing in Caddy yet there is no other version to hand the
 * remaining traffic to, so `baseline` seeds the previous one at 100% and the
 * shift takes its slice out of that. Omitting it — as a first draft of this
 * test did — makes a 10% shift read back as 100%, which is correct behaviour
 * for a table that started empty and the wrong call for a canary.
 */
const shift = (version: string, weight: number, previousVersion = "v1") =>
  Effect.runPromise(
    Effect.gen(function*() {
      const router = yield* RouterPort
      yield* router.setTrafficWeight({ service: SERVICE, version, weight, previousVersion })
      return yield* router.readState(SERVICE)
    }).pipe(Effect.provide(layer))
  )

/** What Caddy itself thinks, read straight off the admin API. */
const storedWeights = async (): Promise<ReadonlyArray<{ dial: string; weight: number }>> => {
  const response = await fetch(`${ADMIN}/id/${CaddyRouter.routeId(SERVICE)}`)
  if (!response.ok) throw new Error(`admin API returned ${response.status}`)
  const route = await response.json() as {
    handle: ReadonlyArray<{
      upstreams?: ReadonlyArray<{ dial: string }>
      load_balancing?: { selection_policy?: { weights?: ReadonlyArray<number> } }
    }>
  }
  const handler = route.handle[0]!
  const weights = handler.load_balancing?.selection_policy?.weights ?? []
  return (handler.upstreams ?? []).map((upstream, index) => ({ dial: upstream.dial, weight: weights[index]! }))
}

describe.skipIf(!REAL)("Caddy adapter against a real Caddy", () => {
  // Per test, not once: Caddy is shared external state, so without this the
  // tests read each other's leftovers and only pass in one order. A double
  // never surfaces that — this is the first thing driving the real thing taught.
  beforeEach(async () => {
    await fetch(`${ADMIN}/id/${CaddyRouter.routeId(SERVICE)}`, { method: "DELETE" }).catch(() => undefined)
  })

  it("creates its route on the first shift, then patches it, through a canary's sequence", async () => {
    // 10% — no route yet, so this is the POST path.
    const atTen = await shift("v2", 10)
    expect(Object.fromEntries(atTen.map((w) => [w.version, w.weight]))).toEqual({ v1: 90, v2: 10 })
    expect(await storedWeights()).toEqual([
      { dial: "api-v1:80", weight: 90 },
      { dial: "api-v2:80", weight: 10 }
    ])

    // 50% and 100% — the route exists, so these are PATCHes of handle/0.
    const atFifty = await shift("v2", 50)
    expect(Object.fromEntries(atFifty.map((w) => [w.version, w.weight]))).toEqual({ v1: 50, v2: 50 })

    const atHundred = await shift("v2", 100)
    expect(Object.fromEntries(atHundred.map((w) => [w.version, w.weight]))).toEqual({ v2: 100 })
    // At 100% the old version is gone from the upstream list entirely.
    expect(await storedWeights()).toEqual([{ dial: "api-v2:80", weight: 100 }])
  }, 30_000)

  it("reads its state back from Caddy rather than from memory", async () => {
    await shift("v2", 50)
    // A fresh layer: nothing carried over, the whole table comes from Caddy.
    const state = await Effect.runPromise(
      Effect.flatMap(RouterPort, (router) => router.readState(SERVICE)).pipe(Effect.provide(layer))
    )
    expect(Object.fromEntries(state.map((w) => [w.version, w.weight]))).toEqual({ v1: 50, v2: 50 })
  }, 30_000)

  it("leaves Caddy serving the route it was given, not merely storing it", async () => {
    await shift("v2", 100)
    // Proxied through Caddy to the upstream flux chose. A config Caddy accepts
    // but cannot serve would pass every assertion above and fail here.
    const response = await fetch(PROXY)
    expect(response.status).toBe(200)
    expect(await response.text()).toContain("Hostname")
  }, 30_000)

  it("rolls traffic back to the previous version", async () => {
    await shift("v2", 100)
    // The saga's compensation: everything back to v1.
    const restored = await shift("v1", 100, "v2")
    expect(Object.fromEntries(restored.map((w) => [w.version, w.weight]))).toEqual({ v1: 100 })
    expect(await storedWeights()).toEqual([{ dial: "api-v1:80", weight: 100 }])
  }, 30_000)
})
