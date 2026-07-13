import { Layer, ManagedRuntime, Redacted } from "effect"
import { NodeChildProcessSpawner, NodeFileSystem, NodeHttpClient, NodePath } from "@effect/platform-node"
import { HttpHealth, NginxRouter, PrometheusMetrics, SlackNotify } from "@flux/adapters"
import { type AppServices, createActivities, type DeploymentInput, type DeploymentResult, SEARCH_ATTRIBUTES } from "@flux/orchestration"
import { TestWorkflowEnvironment } from "@temporalio/testing"
import { Worker } from "@temporalio/worker"
import { afterAll, beforeAll, describe, expect, it } from "vitest"
import { createServer, type Server } from "node:http"
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

// The real adapters, pointed at a local HTTP double and a no-op reload command.
const appLayer = (baseUrl: string, configPath: string): Layer.Layer<AppServices> =>
  Layer.mergeAll(
    PrometheusMetrics.layer({ url: baseUrl }),
    HttpHealth.layer({ url: () => `${baseUrl}/health` }),
    SlackNotify.layer({ webhookUrl: Redacted.make(`${baseUrl}/notify`) }),
    NginxRouter.layer({
      configPath,
      reloadCommand: ["true"],
      address: (service, version) => `${service}-${version}:8080`
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

beforeAll(async () => {
  server = createServer((req, res) => {
    if (req.url?.startsWith("/health")) {
      healthHits++
      res.writeHead(200).end("ok")
    } else if (req.url?.startsWith("/api/v1/query")) {
      metricsHits++
      // Empty vector → the adapter reads 0, which stays within the failure budget.
      res.writeHead(200, { "content-type": "application/json" }).end(
        JSON.stringify({ status: "success", data: { result: [] } })
      )
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
  }).catch(() => {})
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
})
