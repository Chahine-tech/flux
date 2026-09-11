import { DateTime, Effect } from "effect"
import { HttpApiBuilder } from "effect/unstable/httpapi"
import { FluxApi, InvalidRolloutPlan, OutsideDeploymentWindow } from "@flux/contracts"
import { compileRolloutPlan, evaluateWindow, type PlanCompilation } from "@flux/domain"
import type { MultiServiceInput } from "@flux/orchestration"
import { AdmissionController } from "../admission.ts"
import { ReadModel } from "../read-model.ts"
import { TemporalClient } from "../temporal-client.ts"

/** Turn a compile failure into something an operator can act on. */
const describePlanFailure = (
  failure: Exclude<PlanCompilation, { _tag: "Compiled" }>
): { reason: "Cycle" | "UnknownDependency" | "SelfDependency"; detail: string } => {
  switch (failure._tag) {
    case "Cycle":
      return { reason: "Cycle", detail: `dependency cycle: ${failure.path.join(" -> ")}` }
    case "UnknownDependency":
      return {
        reason: "UnknownDependency",
        detail: `"${failure.service}" depends on "${failure.dependsOn}", which is not in this rollout`
      }
    case "SelfDependency":
      return { reason: "SelfDependency", detail: `"${failure.service}" depends on itself` }
  }
}

/** How many deployments `GET /deployments` returns when no `limit` is given. */
const DEFAULT_LIMIT = 20

/**
 * Implementation of the `deployments` group against Temporal.
 *
 * Each handler is a thin translation: decode has already happened (the payload,
 * params and query arrive typed from the shared `FluxApi` schemas), so a handler
 * only calls the `TemporalClient` port and shapes the result. Typed errors
 * (`DeploymentNotFound`, `DeploymentNotActionable`) flow straight back to the
 * HTTP layer, which renders them at the status code declared in the contract.
 */
export const DeploymentsHandlers = HttpApiBuilder.group(FluxApi, "deployments", (handlers) =>
  handlers
    .handle("trigger", ({ payload }) =>
      Effect.gen(function*() {
        const admission = yield* AdmissionController
        const temporal = yield* TemporalClient
        // Temporal gate, before the STM reservation: reject a deploy
        // outside its window with the next opening time. `window` is
        // control-plane-only policy — stripped here so it never reaches the
        // workflow or its history.
        const { window, ...input } = payload
        const decision = evaluateWindow(window, yield* DateTime.nowAsDate)
        if (decision._tag === "Closed") {
          return yield* new OutsideDeploymentWindow({
            service: payload.service,
            window: window!,
            nextAllowed: decision.nextAllowed.toISOString()
          })
        }
        // Reserve a slot (may reject 429/409); free it again if the start fails.
        yield* admission.admit(payload.service)
        const workflowId = yield* temporal.start(input).pipe(
          Effect.onError(() => admission.release(payload.service))
        )
        return { workflowId }
      }))
    .handle("triggerMulti", ({ payload }) =>
      Effect.gen(function*() {
        const admission = yield* AdmissionController
        const temporal = yield* TemporalClient
        // Compile the declared dependencies into a topological plan here, on
        // the Effect side. The workflow receives the plan, never the graph:
        // that keeps the parent Effect-free (D6) and freezes the ordering in
        // the start event, so a later edit to the declaration cannot reorder a
        // replay. `dependsOn` is control-plane-only input and is dropped.
        const { dependsOn, ...rest } = payload
        const compiled = compileRolloutPlan(
          payload.services.map((service) => service.service),
          dependsOn
        )
        if (compiled._tag !== "Compiled") {
          return yield* new InvalidRolloutPlan(describePlanFailure(compiled))
        }
        // Admission covers the whole rollout, all or nothing. Without this the
        // parent starts its children with `startChild`, which never comes back
        // through the control plane, so a twenty-service rollout would walk
        // straight past the global budget. Compiling first means a rollout with
        // a bad dependency graph is rejected before it takes any seats.
        const services = payload.services.map((service) => service.service)
        yield* admission.admitAll(services)
        const workflowId = yield* temporal.startMulti({
          ...(rest as unknown as MultiServiceInput),
          plan: compiled.plan
        }).pipe(Effect.onError(() => admission.releaseAll(services)))
        return { workflowId }
      }))
    .handle("enableDrift", ({ payload }) =>
      Effect.gen(function*() {
        const temporal = yield* TemporalClient
        const scheduleId = yield* temporal.ensureDriftSchedule(payload.service, payload.version, payload.everyMs)
        return { scheduleId }
      }))
    .handle("disableDrift", ({ params }) =>
      Effect.gen(function*() {
        const temporal = yield* TemporalClient
        yield* temporal.disableDrift(params.service)
      }))
    .handle("list", ({ query }) =>
      Effect.gen(function*() {
        const temporal = yield* TemporalClient
        return yield* temporal.list(query.service, query.limit ?? DEFAULT_LIMIT)
      }))
    .handle("status", ({ params }) =>
      Effect.gen(function*() {
        const temporal = yield* TemporalClient
        return yield* temporal.status(params.workflowId)
      }))
    .handle("approve", ({ params }) =>
      Effect.gen(function*() {
        const temporal = yield* TemporalClient
        yield* temporal.approve(params.workflowId)
      }))
    .handle("abort", ({ params }) =>
      Effect.gen(function*() {
        const temporal = yield* TemporalClient
        yield* temporal.abort(params.workflowId)
      })))

/** Implementation of the `stats` group against the CQRS read model. */
export const StatsHandlers = HttpApiBuilder.group(FluxApi, "stats", (handlers) =>
  handlers.handle("stats", () =>
    Effect.gen(function*() {
      const readModel = yield* ReadModel
      const services = yield* readModel.stats()
      return { services }
    })))
