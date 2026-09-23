import { Effect, Layer } from "effect"
import { HttpRouter, HttpServer } from "effect/unstable/http"
import { HttpApiBuilder } from "effect/unstable/httpapi"
import { afterAll, describe, expect, it } from "vitest"
import { HealthApi, HealthHandlers } from "../src/http/health.ts"
import { TemporalClient } from "../src/temporal-client.ts"

/**
 * The two probes, and the difference between them.
 *
 * The assertion that matters is the pair: with Temporal unreachable, readiness
 * must fail *and* liveness must not. Tying liveness to Temporal would turn one
 * outage into a restart loop, and each restart would throw away the in-memory
 * admission state (D41) for deployments still running.
 */

const stub = (reachable: boolean) =>
  Layer.succeed(TemporalClient, {
    start: () => Effect.succeed("wf"),
    startMulti: () => Effect.succeed("multi"),
    status: () => Effect.die("unused"),
    list: () => Effect.succeed([]),
    listRunningIds: () => Effect.succeed([]),
    listClosed: () => Effect.succeed([]),
    approve: () => Effect.void,
    abort: () => Effect.void,
    recordTaskOutcome: () => Effect.void,
    ensureDriftSchedule: () => Effect.succeed("s"),
    disableDrift: () => Effect.void,
    reachable: Effect.succeed(reachable)
  } as unknown as typeof TemporalClient.Service)

const app = (reachable: boolean) =>
  HttpRouter.toWebHandler(
    HttpApiBuilder.layer(HealthApi).pipe(
      Layer.provide(HealthHandlers),
      HttpRouter.provideRequest(stub(reachable)),
      Layer.provide(HttpServer.layerServices)
    )
  )

const healthy = app(true)
const broken = app(false)
afterAll(async () => {
  await healthy.dispose()
  await broken.dispose()
})

const get = (from: typeof healthy, path: string) => from.handler(new Request(`http://localhost${path}`))

describe("health probes", () => {
  it("reports ready when Temporal is reachable and its namespace registered", async () => {
    const res = await get(healthy, "/health/ready")
    expect(res.status).toBe(200)
    expect(await res.json()).toEqual({ status: "ready" })
  })

  it("reports 503 when Temporal is unreachable", async () => {
    const res = await get(broken, "/health/ready")
    expect(res.status).toBe(503)
    expect(await res.json()).toMatchObject({ _tag: "NotReady" })
  })

  it("stays live while Temporal is down, so the probe cannot cause a restart loop", async () => {
    const res = await get(broken, "/health/live")
    expect(res.status).toBe(200)
    expect(await res.json()).toEqual({ status: "live" })
  })

  it("needs no bearer token — a kubelet does not carry one", async () => {
    // The app under test has no auth layer at all, which is the point: these
    // endpoints are outside FluxApi precisely so the middleware cannot reach
    // them. A 401 here would mean they had been folded back in.
    for (const path of ["/health/live", "/health/ready"]) {
      expect((await get(healthy, path)).status).not.toBe(401)
    }
  })
})
