import { Layer, ManagedRuntime, Redacted } from "effect"
import { NodeChildProcessSpawner, NodeFileSystem, NodeHttpClient, NodePath } from "@effect/platform-node"
import { CaddyRouter, HttpHealth, NginxRouter, PrometheusMetrics, SlackNotify } from "@flux/adapters"
import { type AppServices, createActivities, type DeploymentInput, type DeploymentResult, SEARCH_ATTRIBUTES } from "@flux/orchestration"
import { TestWorkflowEnvironment } from "@temporalio/testing"
import { Worker } from "@temporalio/worker"
import { afterAll, beforeAll, describe, expect, it } from "vitest"
import { createServer, type IncomingMessage, type Server } from "node:http"
import type { AddressInfo } from "node:net"
import { readFileSync, rmSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { fileURLToPath } from "node:url"

/**
 * The full worker stack, integrated: the real adapters (HTTP health probe,
 * Prometheus query, nginx config write + reload, Slack POST) run inside the real
 * activities and the real `ManagedRuntime`, driving the real workflow bundle on
 * a real (time-skipping) Temporal server. A canary actually completes.
 *
 * The only test doubles are the external endpoints — a local HTTP server standing
 * in for the target service / Prometheus / Slack, and `true` as the nginx reload
 * command — because those are the things you genuinely can't run in a unit test.
 * Everything flux owns is the real code path, and we assert the real side effects
 * (the health endpoint was probed, the nginx config was written).
 */

const KEYWORD = 2
const TASK_QUEUE = "flux-integration"
const workflowsPath = fileURLToPath(import.meta.resolve("@flux/orchestration/workflows"))

const SpawnerLayer = NodeChildProcessSpawner.layer.pipe(
  Layer.provide(Layer.mergeAll(NodeFileSystem.layer, NodePath.layer))
)
const PlatformLayer = Layer.mergeAll(NodeHttpClient.layerUndici, NodeFileSystem.layer, SpawnerLayer)

const backendAddress = (service: string, version: string) => `${service}-${version}:8080`
const versionOfDial = (service: string, dial: string) =>
  dial.startsWith(`${service}-`) ? dial.slice(service.length + 1).replace(/:\d+$/, "") : undefined

// The real adapters, pointed at a local HTTP double and a no-op reload command.
const appLayer = (baseUrl: string, configPath: string): Layer.Layer<AppServices> =>
  Layer.mergeAll(
    PrometheusMetrics.layer({ url: baseUrl }),
    HttpHealth.layer({ url: () => `${baseUrl}/health` }),
    SlackNotify.layer({ webhookUrl: Redacted.make(`${baseUrl}/notify`) }),
    NginxRouter.layer({
      configPath,
      reloadCommand: ["true"],
      address: backendAddress
    })
  ).pipe(Layer.provide(PlatformLayer))

// The same stack with the Caddy adapter driving the admin-API double (D20).
const caddyAppLayer = (baseUrl: string): Layer.Layer<AppServices> =>
  Layer.mergeAll(
    PrometheusMetrics.layer({ url: baseUrl }),
    HttpHealth.layer({ url: () => `${baseUrl}/health` }),
    SlackNotify.layer({ webhookUrl: Redacted.make(`${baseUrl}/notify`) }),
    CaddyRouter.layer({
      adminUrl: baseUrl,
      server: "flux",
      address: backendAddress,
      versionOf: versionOfDial
    })
  ).pipe(Layer.provide(PlatformLayer))

const input: DeploymentInput = {
  service: "checkout",
  version: "v2",
  previousVersion: "v1",
  steps: [
    { percent: 10, monitorMs: 100, requiresApproval: false },
    { percent: 100, monitorMs: 100, requiresApproval: false }
  ],
  rules: [{ name: "errorRate", query: "sum(rate(errors[1m]))", max: 0.01 }],
  pollIntervalMs: 50
}

let env: TestWorkflowEnvironment
let server: Server
let baseUrl: string
let healthHits = 0
let metricsHits = 0

// A stand-in for Caddy's admin API: an in-memory route store keyed by @id,
// recording every handler write so the test can assert the weight sequence.
const caddyRoutes = new Map<string, { "@id": string; handle: Array<unknown> }>()
const caddyWrites: Array<unknown> = []

const readBody = (req: IncomingMessage): Promise<unknown> =>
  new Promise((resolve, reject) => {
    const chunks: Array<Buffer> = []
    req.on("data", (chunk) => chunks.push(chunk))
    req.on("end", () => {
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString("utf8")))
      } catch (error) {
        reject(error)
      }
    })
  })

beforeAll(async () => {
  server = createServer((req, res) => {
    const idMatch = /^\/id\/([^/]+)(\/handle\/0)?$/.exec(req.url ?? "")
    if (req.url?.startsWith("/health")) {
      healthHits++
      res.writeHead(200).end("ok")
    } else if (req.url?.startsWith("/api/v1/query")) {
      metricsHits++
      // Empty vector → the adapter reads 0, which stays within the failure budget.
      res.writeHead(200, { "content-type": "application/json" }).end(
        JSON.stringify({ status: "success", data: { result: [] } })
      )
    } else if (req.method === "GET" && idMatch) {
      const route = caddyRoutes.get(idMatch[1]!)
      if (route === undefined) res.writeHead(404).end()
      else res.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify(route))
    } else if (req.method === "PATCH" && idMatch?.[2] !== undefined) {
      void readBody(req).then((handler) => {
        const route = caddyRoutes.get(idMatch[1]!)
        if (route === undefined) return res.writeHead(404).end()
        route.handle[0] = handler
        caddyWrites.push(handler)
        res.writeHead(200).end()
      })
    } else if (req.method === "POST" && req.url === "/config/apps/http/servers/flux/routes") {
      void readBody(req).then((body) => {
        const route = body as { "@id": string; handle: Array<unknown> }
        caddyRoutes.set(route["@id"], route)
        caddyWrites.push(route.handle[0])
        res.writeHead(200).end()
      })
    } else {
      res.writeHead(200).end("ok") // notify webhook
    }
  })
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve))
  baseUrl = `http://127.0.0.1:${(server.address() as AddressInfo).port}`

  env = await TestWorkflowEnvironment.createTimeSkipping()
  await env.connection.operatorService.addSearchAttributes({
    namespace: env.namespace ?? "default",
    searchAttributes: {
      [SEARCH_ATTRIBUTES.service]: KEYWORD,
      [SEARCH_ATTRIBUTES.version]: KEYWORD,
      [SEARCH_ATTRIBUTES.status]: KEYWORD
    }
  }).catch((error: unknown) => {
    // Ignore "already registered"; surface anything unexpected.
    if (!/already exist/i.test(String(error))) throw error
  })
}, 60_000)

afterAll(async () => {
  await env?.teardown()
  await new Promise<void>((resolve) => server.close(() => resolve()))
})

describe("worker integration", () => {
  it("runs a real canary through the real adapters to Succeeded", async () => {
    const configPath = join(tmpdir(), `flux-nginx-${Date.now()}-${Math.random().toString(36).slice(2)}.conf`)
    const runtime = ManagedRuntime.make(appLayer(baseUrl, configPath))
    const worker = await Worker.create({
      connection: env.nativeConnection,
      namespace: env.namespace ?? "default",
      taskQueue: TASK_QUEUE,
      workflowsPath,
      activities: createActivities(runtime)
    })

    try {
      const result = await worker.runUntil(
        env.client.workflow.execute("deploymentWorkflow", {
          taskQueue: TASK_QUEUE,
          workflowId: `int-${Date.now()}`,
          args: [input]
        })
      ) as DeploymentResult

      expect(result.kind).toBe("Succeeded")
      // The real HTTP health adapter actually probed the target.
      expect(healthHits).toBeGreaterThan(0)
      // The real Prometheus adapter actually queried metrics during monitoring.
      expect(metricsHits).toBeGreaterThan(0)
      // The real nginx adapter actually rendered and wrote a config for the new version.
      expect(readFileSync(configPath, "utf8")).toContain("checkout-v2:8080")
    } finally {
      await runtime.dispose()
      rmSync(configPath, { force: true })
    }
  }, 90_000)

  it("runs the same canary through the Caddy adapter, proving the port (D20)", async () => {
    const runtime = ManagedRuntime.make(caddyAppLayer(baseUrl))
    const worker = await Worker.create({
      connection: env.nativeConnection,
      namespace: env.namespace ?? "default",
      taskQueue: TASK_QUEUE,
      workflowsPath,
      activities: createActivities(runtime)
    })

    try {
      const result = await worker.runUntil(
        env.client.workflow.execute("deploymentWorkflow", {
          taskQueue: TASK_QUEUE,
          workflowId: `int-caddy-${Date.now()}`,
          args: [{ ...input, service: "cart" }]
        })
      ) as DeploymentResult

      expect(result.kind).toBe("Succeeded")
      // First shift (10%): the adapter had no state for the service, so the
      // previous version was seeded — 90/10, not a single canary upstream.
      const first = caddyWrites[0] as {
        upstreams: Array<{ dial: string }>
        load_balancing: { selection_policy: { weights: Array<number> } }
      }
      expect(first.upstreams.map((u) => u.dial)).toEqual(["cart-v1:8080", "cart-v2:8080"])
      expect(first.load_balancing.selection_policy.weights).toEqual([90, 10])
      // Full rollout: the managed route ends at 100% on the new version.
      const last = caddyWrites.at(-1) as typeof first
      expect(last.upstreams.map((u) => u.dial)).toEqual(["cart-v2:8080"])
      expect(last.load_balancing.selection_policy.weights).toEqual([100])
    } finally {
      await runtime.dispose()
    }
  }, 90_000)
})
