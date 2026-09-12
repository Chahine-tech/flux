import { type ChildWorkflowHandle, defineQuery, log, setHandler, startChild, workflowInfo } from "@temporalio/workflow"
import type {
  DeploymentInput,
  MultiServiceInput,
  MultiServiceOutcome,
  MultiServiceResult,
  MultiServiceState,
  RolloutFailurePolicy,
  RolloutPlanInput
} from "../deployment-input.ts"
import { abortSignal, deploymentWorkflow } from "./deployment.workflow.ts"

/**
 * Multi-service rollout — deterministic parent over N `deploymentWorkflow`
 * children, one per service, ordered by their declared dependencies.
 *
 * The dependency graph is compiled **before** the workflow starts and arrives
 * as `input.plan`: a topological order plus flat lookup tables. Nothing here
 * traverses a graph, so the parent stays plain TypeScript (D6) and the
 * ordering is frozen in history rather than recomputed at replay.
 *
 * Scheduling is one promise per service: wait for the dependencies to settle,
 * take a concurrency slot, run the child. Because `plan.order` is topological,
 * a service's dependencies always have their promise registered before it asks
 * for it.
 *
 * Failure handling is `input.onFailure` (see `RolloutFailurePolicy`). A service
 * blocked by an upstream failure is reported `Skipped`, not `Failed` — no child
 * ever ran for it.
 */

/** Read the aggregate rollout state. */
export const multiStatusQuery = defineQuery<MultiServiceState>("status")

type Child = ChildWorkflowHandle<typeof deploymentWorkflow>

/** Every service independent — the shape of a rollout declared without dependencies. */
const independentPlan = (services: ReadonlyArray<string>): RolloutPlanInput => {
  const dependsOn: Record<string, ReadonlyArray<string>> = {}
  const transitiveDependents: Record<string, ReadonlyArray<string>> = {}
  for (const service of services) {
    dependsOn[service] = []
    transitiveDependents[service] = []
  }
  return { order: [...services], dependsOn, transitiveDependents, waves: [[...services]] }
}

/**
 * FIFO concurrency limiter. Deterministic by construction: waiters resume in
 * the order they queued, and that order is a consequence of child completions,
 * which Temporal replays identically.
 */
const makeSlots = (limit: number) => {
  let taken = 0
  const waiting: Array<() => void> = []
  return {
    acquire: async (): Promise<void> => {
      if (taken < limit) {
        taken++
        return
      }
      await new Promise<void>((resolve) => waiting.push(resolve))
      taken++
    },
    release: (): void => {
      taken--
      waiting.shift()?.()
    }
  }
}

export async function multiServiceDeployment(input: MultiServiceInput): Promise<MultiServiceResult> {
  const parentId = workflowInfo().workflowId
  const byName = new Map<string, DeploymentInput>(input.services.map((service) => [service.service, service]))
  const plan = input.plan ?? independentPlan(input.services.map((service) => service.service))
  const policy: RolloutFailurePolicy = input.onFailure ?? (input.failFast ? "fail-fast" : "continue")

  const outcomes = new Map<string, MultiServiceOutcome>()
  const inflight = new Map<string, Child>()
  /** Services that must not start, and the upstream that stopped them. */
  const blocked = new Map<string, string>()

  let state: MultiServiceState = {
    total: input.services.length,
    running: 0,
    succeeded: 0,
    failed: 0,
    skipped: 0
  }
  setHandler(multiStatusQuery, () => state)

  const abort = async (service: string): Promise<void> => {
    const handle = inflight.get(service)
    if (handle === undefined) return
    await handle
      .signal(abortSignal)
      .catch((error) => log.warn("abort failed", { service, error: String(error) }))
  }

  /** Apply the failure policy to everything still to come. */
  const spread = async (failed: string): Promise<void> => {
    if (policy === "continue") return

    const affected = policy === "fail-fast"
      // Everything else in the rollout, matching the original behaviour.
      ? plan.order.filter((service) => service !== failed)
      // Only what actually depends on the failure; siblings are untouched.
      : (plan.transitiveDependents[failed] ?? [])

    for (const service of affected) {
      if (outcomes.has(service) || service === failed) continue
      if (!blocked.has(service)) blocked.set(service, failed)
      await abort(service)
    }
  }

  const settle = (service: string, outcome: MultiServiceOutcome): void => {
    outcomes.set(service, outcome)
    state = {
      ...state,
      succeeded: state.succeeded + (outcome.kind === "Succeeded" ? 1 : 0),
      failed: state.failed + (outcome.kind === "Succeeded" || outcome.kind === "Skipped" ? 0 : 1),
      skipped: state.skipped + (outcome.kind === "Skipped" ? 1 : 0)
    }
  }

  const slots = makeSlots(Math.max(1, Math.min(input.maxConcurrency, input.services.length)))
  const settled = new Map<string, Promise<void>>()

  const run = async (service: string): Promise<void> => {
    // `plan.order` is topological, so these are already registered.
    for (const dependency of plan.dependsOn[service] ?? []) {
      await settled.get(dependency)
    }

    if (blocked.has(service)) {
      settle(service, { kind: "Skipped", service, blockedBy: blocked.get(service)! })
      return
    }

    await slots.acquire()
    try {
      // Re-check: the rollout may have turned while this waited for a slot.
      if (blocked.has(service)) {
        settle(service, { kind: "Skipped", service, blockedBy: blocked.get(service)! })
        return
      }

      const serviceInput = byName.get(service)
      if (serviceInput === undefined) {
        settle(service, { kind: "Failed", service, reason: "service is in the plan but not in the rollout" })
        return
      }

      const handle = await startChild(deploymentWorkflow, {
        workflowId: `${parentId}-${service}`,
        // Each child carries its own service's fairness key rather than
        // inheriting the parent's. Without this a twenty-service rollout is one
        // tenant on the queue and can crowd out an unrelated single deployment;
        // with it, the queue is shared per service, which is the unit flux
        // already treats as a tenant (admission allows one deployment each).
        priority: { fairnessKey: service },
        args: [serviceInput]
      })
      inflight.set(service, handle)
      state = { ...state, running: state.running + 1 }

      let outcome: MultiServiceOutcome
      try {
        outcome = await handle.result()
      } catch (error) {
        // A child that fails outright rather than reporting an outcome must not
        // take the parent down with it: record it and let the policy decide.
        outcome = { kind: "Failed", service, reason: String(error) }
      }

      state = { ...state, running: state.running - 1 }
      settle(service, outcome)
      if (outcome.kind !== "Succeeded") {
        await spread(service)
      }
    } finally {
      inflight.delete(service)
      slots.release()
    }
  }

  for (const service of plan.order) {
    settled.set(service, run(service))
  }
  await Promise.all(settled.values())

  const perService = plan.order
    .filter((service) => outcomes.has(service))
    .map((service) => ({ service, result: outcomes.get(service)! }))
  const allSucceeded = perService.length > 0 &&
    perService.every((entry) => entry.result.kind === "Succeeded")

  return { kind: allSucceeded ? "AllSucceeded" : "SomeFailed", perService }
}
