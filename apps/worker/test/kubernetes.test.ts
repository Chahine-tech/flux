import { execFile } from "node:child_process"
import { promisify } from "node:util"
import { describe, expect, it } from "vitest"

const run = promisify(execFile)

/**
 * The one thing a Kubernetes cluster is for: a canary survives its worker being
 * evicted by the orchestrator.
 *
 * Everything else about the chart is settled by `helm template` without a
 * cluster, so this is the only assertion that needs one. It is also not the same
 * proof as `worker-kill.test.ts`: that one SIGKILLs a process, this goes through
 * Kubernetes' termination lifecycle, so the Temporal SDK's own drain runs first
 * and a SIGKILL only follows if the grace period expires.
 *
 * Gated, and the plumbing belongs to whoever runs it, the same split as the
 * compose tier, where CI brings up Docker and the test only connects:
 *
 *   (see deploy/kubernetes/README.md for the cluster and chart install)
 *   kubectl port-forward svc/flux-control-plane 18080:8080 &
 *   FLUX_K8S=1 pnpm --filter @flux/worker test
 */
const REAL = process.env.FLUX_K8S === "1"
const CONTROL_PLANE = process.env.FLUX_CONTROL_PLANE ?? "http://localhost:18080"

/**
 * `api`, not `checkout`: the demo exporter publishes `checkout` above its error
 * budget, so a canary there rolls back on its metrics and the eviction would
 * never be the reason for anything.
 */
const SERVICE = "api"
const MONITOR_MS = Number(process.env.FLUX_MONITOR_MS ?? 60_000)

const api = async <A>(path: string, init?: RequestInit): Promise<A> => {
  const response = await fetch(`${CONTROL_PLANE}${path}`, init)
  if (!response.ok) throw new Error(`${path} -> ${response.status} ${await response.text()}`)
  return response.json() as Promise<A>
}

const workerPods = async (): Promise<ReadonlyArray<string>> => {
  const { stdout } = await run("kubectl", [
    "get",
    "pods",
    "-l",
    "app.kubernetes.io/component=worker",
    "-o",
    "jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}"
  ])
  return stdout.split("\n").filter(Boolean)
}

const phaseOf = (workflowId: string) =>
  api<{ phase: string; outcome?: string }>(`/deployments/${workflowId}`)

describe.skipIf(!REAL)("a canary in Kubernetes", () => {
  it("completes after the worker running it is evicted", async () => {
    await expect.poll(() => api("/health/ready").then(() => true, () => false), { timeout: 60_000 })
      .toBe(true)

    const { workflowId } = await api<{ workflowId: string }>("/deployments", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        service: SERVICE,
        version: "v3",
        previousVersion: "v2",
        strategy: {
          kind: "canary",
          steps: [
            // Long enough that the monitor activity is genuinely in flight when
            // the pod goes away.
            { percent: 10, monitorMs: MONITOR_MS, requiresApproval: false },
            { percent: 100, monitorMs: 0, requiresApproval: false }
          ]
        },
        rules: [{
          name: "errorRate",
          query: `sum(rate(http_requests_total{service="${SERVICE}",status=~"5.."}[1m]))` +
            ` / sum(rate(http_requests_total{service="${SERVICE}"}[1m]))`,
          max: 0.01
        }],
        pollIntervalMs: 5_000
      })
    })

    await expect.poll(() => phaseOf(workflowId).then((s) => s.phase), { timeout: 120_000 })
      .toBe("monitoring")

    const before = await workerPods()
    expect(before.length).toBeGreaterThan(0)
    const victim = before[0]!
    await run("kubectl", ["delete", "pod", victim, "--wait=false"])

    await expect.poll(() => phaseOf(workflowId).then((s) => s.outcome), { timeout: 300_000 })
      .toBe("Succeeded")

    // Not just "it finished": the pod really went, so the work really moved.
    await expect.poll(() => workerPods().then((pods) => pods.includes(victim)), { timeout: 120_000 })
      .toBe(false)
  }, 600_000)
})
