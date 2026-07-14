import { Effect, Layer } from "effect"
import { HttpRouter, HttpServer } from "effect/unstable/http"
import { HttpApiBuilder } from "effect/unstable/httpapi"
import { DeploymentNotFound, FluxApi } from "@flux/contracts"
import type { ServiceStats } from "@flux/contracts"
import { afterAll, describe, expect, it } from "vitest"
import * as Admission from "../src/admission.ts"
import { DeploymentsHandlers, StatsHandlers } from "../src/http/handlers.ts"
import { ReadModel } from "../src/read-model.ts"
import { TemporalClient } from "../src/temporal-client.ts"

/**
 * Exercises the HTTP layer as a real fetch handler against a mock `TemporalClient`
 * — no cluster, no socket. It pins the wiring the live smoke test can't assert
 * deterministically: routing, payload/param decoding, status codes, and that a
 * typed `DeploymentNotFound` from the port is rendered as the 404 the contract
 * declares. `toWebHandler` gives a `Request → Response` function, so the tests
 * are plain async assertions over the exact bytes a client would receive.
 */

const runningState = {
  phase: "monitoring",
  service: "checkout",
  version: "v2",
  currentPercent: 10,
  stepIndex: 0,
  totalSteps: 3
} as const

const MockTemporal = Layer.succeed(TemporalClient, {
  start: (request) => Effect.succeed(`dep-${request.service}-test`),
  startMulti: () => Effect.succeed("multi"),
  status: (workflowId) =>
    workflowId === "known"
      ? Effect.succeed(runningState)
      : Effect.fail(new DeploymentNotFound({ workflowId })),
  list: () =>
    Effect.succeed([{ workflowId: "dep-checkout-1", status: "RUNNING", startTime: "2026-07-13T00:00:00.000Z" }]),
  listRunningIds: () => Effect.succeed(["dep-checkout-1"]),
  listClosed: () => Effect.succeed([]),
  approve: () => Effect.void,
  abort: () => Effect.void,
  ensureDriftSchedule: () => Effect.succeed("flux-drift-api")
})

const sampleStats: ServiceStats = {
  service: "checkout",
  total: 4,
  succeeded: 3,
  rolledBack: 1,
  aborted: 0,
  failed: 0,
  rollbackRate: 0.25,
  meanDurationMs: 42_000
}

const MockReadModel = Layer.succeed(ReadModel, {
  stats: () => Effect.succeed([sampleStats])
})

// The full app layer, self-contained: handlers + their per-request dependencies
// (mock `TemporalClient` / `ReadModel`) + the HTTP platform. The dependencies are
// provided with `provideRequest` (the real server provides the live services the
// same way, at the request boundary).
const AppLive = HttpApiBuilder.layer(FluxApi).pipe(
  Layer.provide(DeploymentsHandlers),
  Layer.provide(StatsHandlers),
  HttpRouter.provideRequest(Layer.mergeAll(MockTemporal, MockReadModel, Admission.layer(100))),
  Layer.provide(HttpServer.layerServices)
)

const { dispose, handler } = HttpRouter.toWebHandler(AppLive)
afterAll(() => dispose())

const url = (path: string) => `http://localhost${path}`
const post = (path: string, body?: unknown) =>
  handler(
    new Request(url(path), {
      method: "POST",
      ...(body === undefined ? {} : { body: JSON.stringify(body), headers: { "content-type": "application/json" } })
    })
  )

const validTrigger = {
  service: "checkout",
  version: "v2",
  previousVersion: "v1",
  steps: [{ percent: 10, monitorMs: 60_000, requiresApproval: false }],
  rules: [{ name: "error_rate", query: "sum(rate(errors[1m]))", max: 0.01 }],
  pollIntervalMs: 5_000
} as const

describe("control plane HTTP API", () => {
  it("POST /deployments starts a workflow and returns its id", async () => {
    const res = await post("/deployments", validTrigger)
    expect(res.status).toBe(200)
    expect(await res.json()).toEqual({ workflowId: "dep-checkout-test" })
  })

  it("POST /deployments rejects a malformed body with 400", async () => {
    const res = await post("/deployments", { service: "", version: "v2", steps: [], rules: [], pollIntervalMs: 0 })
    expect(res.status).toBe(400)
  })

  it("POST /deployments rejects a second concurrent deployment of the same service (409)", async () => {
    const request = { ...validTrigger, service: "billing" }
    expect((await post("/deployments", request)).status).toBe(200)
    const second = await post("/deployments", request)
    expect(second.status).toBe(409)
    expect(await second.json()).toMatchObject({ _tag: "ServiceAlreadyDeploying", service: "billing" })
  })

  it("GET /deployments lists deployments", async () => {
    const res = await handler(new Request(url("/deployments")))
    expect(res.status).toBe(200)
    const list = await res.json() as ReadonlyArray<{ workflowId: string }>
    expect(list).toHaveLength(1)
    expect(list[0]?.workflowId).toBe("dep-checkout-1")
  })

  it("GET /deployments/:id returns the live state", async () => {
    const res = await handler(new Request(url("/deployments/known")))
    expect(res.status).toBe(200)
    expect(await res.json()).toMatchObject({ phase: "monitoring", currentPercent: 10 })
  })

  it("GET /deployments/:id renders DeploymentNotFound as 404", async () => {
    const res = await handler(new Request(url("/deployments/ghost")))
    expect(res.status).toBe(404)
    expect(await res.json()).toMatchObject({ _tag: "DeploymentNotFound", workflowId: "ghost" })
  })

  it("POST /deployments/:id/approve succeeds with no content", async () => {
    const res = await post("/deployments/known/approve")
    expect(res.status).toBe(204)
  })

  it("POST /deployments/multi starts a multi-service rollout", async () => {
    const res = await post("/deployments/multi", {
      services: [validTrigger, { ...validTrigger, service: "web" }],
      maxConcurrency: 2,
      failFast: true
    })
    expect(res.status).toBe(200)
    expect(await res.json()).toEqual({ workflowId: "multi" })
  })

  it("POST /drift enables a drift-check schedule for a service", async () => {
    const res = await post("/drift", { service: "api", version: "v2", everyMs: 60_000 })
    expect(res.status).toBe(200)
    expect(await res.json()).toEqual({ scheduleId: "flux-drift-api" })
  })

  it("GET /stats returns the read model's aggregations", async () => {
    const res = await handler(new Request(url("/stats")))
    expect(res.status).toBe(200)
    expect(await res.json()).toEqual({ services: [sampleStats] })
  })
})
