import { Effect } from "effect"
import { HttpApiBuilder } from "effect/unstable/httpapi"
import { FluxApi } from "@flux/contracts"
import { ReadModel } from "../read-model.ts"
import { TemporalClient } from "../temporal-client.ts"

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
        const temporal = yield* TemporalClient
        const workflowId = yield* temporal.start(payload)
        return { workflowId }
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

/** Implementation of the `stats` group against the CQRS read model (D12). */
export const StatsHandlers = HttpApiBuilder.group(FluxApi, "stats", (handlers) =>
  handlers.handle("stats", () =>
    Effect.gen(function*() {
      const readModel = yield* ReadModel
      const services = yield* readModel.stats()
      return { services }
    })))
