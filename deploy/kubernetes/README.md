# flux on Kubernetes

flux does not require Kubernetes and does not use its primitives to move
traffic. It drives Caddy itself. This directory puts flux *inside* Kubernetes
as a laboratory: the question is how a durable deployment system behaves when
the orchestrator running it can evict its processes.

```
chart/   the flux chart: control plane, worker, Caddy
deps/    the cluster flux talks to: PostgreSQL, Temporal, Prometheus, the demo app
```

The split is deliberate. The chart is flux. The deps are the lab bench, in plain
manifests rather than the official Temporal chart, which pulls Cassandra or
Elasticsearch by default.

## Running it

Needs a cluster, `helm`, and the two images built.

The images assemble a build, they do not produce one, so the order matters:
`build` compiles, `bundle-workflows` produces the deterministic workflow bundle
the worker loads, and `runtime-manifest` derives the `package.json` the image
installs from. Building inside the image instead would reinstall the whole
workspace on every layer cache miss.

```bash
pnpm --filter @flux/worker build
pnpm --filter @flux/worker bundle-workflows
pnpm --filter @flux/worker runtime-manifest
pnpm --filter @flux/control-plane build
pnpm --filter @flux/control-plane runtime-manifest

docker build -f apps/worker/Dockerfile -t flux-worker:dev .
docker build -f apps/control-plane/Dockerfile -t flux-control-plane:dev .

k3d cluster create flux-lab
k3d image import flux-worker:dev flux-control-plane:dev -c flux-lab

kubectl apply -f deploy/kubernetes/deps/
kubectl wait --for=condition=available --timeout=300s \
  deployment/postgresql deployment/temporal deployment/prometheus \
  deployment/demo-target deployment/demo-metrics
kubectl wait --for=condition=complete --timeout=300s job/temporal-namespace

helm install flux deploy/kubernetes/chart --wait
```

Then drive it with the CLI through a port-forward:

```bash
kubectl port-forward svc/flux-control-plane 18080:8080 &
pnpm --filter @flux/cli build
node apps/cli/dist/main.mjs deploy --service api --version v2 \
  --previous-version v1 --monitor 20s --control-plane http://localhost:18080
```

`api` is healthy in the demo exporter and promotes; `checkout` is published at
8% errors against a 1% budget and rolls back. Both on real Prometheus data.

Tear down with `k3d cluster delete flux-lab`.

## The one experiment

```bash
kubectl port-forward svc/flux-control-plane 18080:8080 &
FLUX_K8S=1 pnpm --filter @flux/worker test
```

`apps/worker/test/kubernetes.test.ts` starts a canary, waits until its monitor
activity is genuinely in flight, deletes the worker pod running it, and asserts
the canary still completes. This is what justifies a cluster: everything else
about the chart is settled by `helm template` without one.

It is gated on `FLUX_K8S=1` and skips otherwise, like the other tests that need
something running. The port-forward stays outside the test, the same way the
compose tier leaves Docker to whoever runs it.

It is not the same proof as the SIGKILL test in `apps/worker/test/`. That one
kills a process; this goes through Kubernetes' termination lifecycle: SIGTERM,
the Temporal SDK's own drain, and a SIGKILL only if the grace period runs out.
Observed recovery: the draining worker returns its activity task to the queue
and it is redelivered as attempt 2 on another worker, with **no heartbeat
timeout and no failure event**, because nothing failed. Faster than the hard-kill
path, and for a different reason: the drain tells the server instead of leaving
it to notice.

## Things the chart says no to, on purpose

**The control plane is one replica, and that is a finding.** Admission control
is a `TxSemaphore` in process memory and the read model is a local SQLite file,
so a second replica means a second budget, a second projection, and a second
poller querying every running workflow. The field is not exposed in `values.yaml`
because exposing it would imply it works.

**The read model volume is an `emptyDir`.** It is a projection the poller
rebuilds from Temporal, so losing it costs a re-projection. A PVC would claim it
is a source of truth.

**Caddy, not nginx.** The nginx adapter renders a config file and signals a
process, so it needs to share a filesystem and a process namespace with nginx.
That means a sidecar in the worker's pod, tying the router's lifecycle to the
worker's scaling. Caddy's admin API is HTTP and crosses pods. Setting `router.type=nginx`
renders without the Caddy resources and is left to whoever wants to try the
sidecar.

**No RBAC rules.** flux never touches the Kubernetes API: it speaks HTTP to
Caddy and gRPC to Temporal. So there is nothing to grant, which is the "no
Kubernetes" claim visible from the permissions side rather than argued.

## Known weak point

The HPA defaults to CPU, and CPU is close to the wrong signal for this workload:
a Temporal worker spends its life blocked in a heartbeating activity, so its CPU
is near zero while its slots are full. The signal that fits is the task-queue
backlog flux already reads over gRPC, but reaching an HPA from there takes three
steps flux has not taken: publish it as a gauge, scrape it, expose it through a
custom-metrics adapter. CPU is there because it needs nothing installed, not
because it is right. Override `worker.autoscaling.metrics` once the gauge exists.

## Seeing the shape of the work without leaving the terminal

```bash
FLUX_TRACE_CONSOLE=1 OTLP_ENDPOINT=http://localhost:4318 pnpm dev
```

Draws each span tree as it finishes, with log lines under the span that emitted
them. It wraps the tracer rather than replacing it, so OTLP export keeps working
and Jaeger still gets everything.

It only ever draws **one process**, which is the limit worth knowing rather than
discovering. flux's interesting trace crosses three, from `flux deploy` through
the control plane into the worker's activities, and Jaeger is where that lives.
What this gives you is the shape of one process's work while you are changing
its code.

## Worker versioning is a migration, not a setting

Setting `worker.extraEnv.FLUX_WORKER_BUILD_ID` turns on Temporal's Worker
Deployment Versioning, and the worker's first poll writes a version into the
*server* that outlives the pods, the Helm release and the cluster. Neither
Kubernetes nor Helm has any idea it exists, which is what makes both of its
failure modes quiet.

**A rolling update strands in-flight work.** Executions are PINNED to the build
that started them, so replacing every pod at once removes the only workers
allowed to run them. A canary mid-rollout does not fail: it stops, and its
status query stops being answerable too, because a query is served by a worker
of the pinned version. It resumes intact seconds after a worker on that build
comes back. Roll one build at a time, or accept the pause.

**Removing the value does not turn versioning off.** Temporal keeps routing the
task queue to the last version it was told was current, so unversioned workers
are invisible to it. What that looks like: pods Ready, probes green, pollers
`POLLING`, Temporal reachable from inside the worker, and not one workflow
making progress. Every status query comes back as a 503 after the client
deadline, because no worker answers.

The server is the only place that knows, so it is the only place to ask:

```bash
kubectl run tctl --rm -i --restart=Never --image=temporalio/admin-tools:1.31.2 \
  --command -- temporal worker deployment describe \
  --address temporal:7233 --name flux-worker
```

If it reports a current version and your workers no longer carry that build id,
that is the state above. Undo it with:

```bash
kubectl run tctl --rm -i --restart=Never --image=temporalio/admin-tools:1.31.2 \
  --command -- temporal worker deployment set-current-version \
  --address temporal:7233 --deployment-name flux-worker --unversioned --yes
```

(`--deployment-name` here, `--name` above. The CLI spells the same argument two
ways depending on the subcommand, and copying one line into the other fails with
`unknown flag`.)

Work resumes immediately; nothing is lost while it is stuck.

## Reconciling the router

Stopping the cluster with a canary in flight showed that a durable workflow and
a non-durable actuator drift apart: Caddy came back with an empty configuration
while flux still reported 10% on the new version. Both were sincere; only one
described traffic. It repaired itself when the next step wrote again, which
works only while a step is still due.

`driftCheck` is the piece that closes that, and the lab can run it now:

```bash
helm upgrade flux deploy/kubernetes/chart \
  --set driftDetection.enabled=true \
  --set driftDetection.services[0].service=api \
  --set driftDetection.services[0].version=v2
```

A post-install job calls `POST /drift`, which creates a Temporal Schedule per
service. Emptying Caddy's routes by hand after that, which is exactly the state
a cluster restart leaves behind:

```bash
kubectl run saboteur --rm -i --restart=Never --image=curlimages/curl:8.11.1 \
  --command -- curl -sS -X PATCH -H 'content-type: application/json' -d '[]' \
  http://caddy:2019/config/apps/http/servers/flux/routes
```

restores `api-v2` at weight 100 within one interval, measured at nine seconds
against a thirty second schedule.

Two things to know. Reconciliation is assertive: a service under drift detection
has any manual routing change undone within an interval, which is why it is off
by default. And turning it off in the chart does not remove the schedule, the
same way removing `FLUX_WORKER_BUILD_ID` does not turn versioning off. The
schedule lives on the server:

```bash
kubectl run tsched --rm -i --restart=Never --image=temporalio/admin-tools:1.31.2 \
  --command -- temporal schedule list --address temporal:7233 --namespace default
```
