import { Duration, Effect, Schema } from "effect"
import { MetricsUnavailable, RouterUnavailable } from "@flux/application"
import { CanaryStep, HealthCheckFailed, Identifier, MetricRule, RolledBack, Succeeded } from "@flux/domain"
import { DurableDeferred, Workflow } from "effect/unstable/workflow"
import { healthCheckActivity, monitorStepActivity, shiftTrafficActivity } from "./activities.ts"

/**
 * The same canary reimplemented on `effect/unstable/workflow` (D23/N7) — not a
 * migration target, a comparison datapoint. Scenarios mirror
 * `deployment.workflow.test.ts`: sequencing, a manual approval gate, and a
 * threshold breach rolling back.
 *
 * Deliberately narrower than the Temporal version: no abort-mid-monitor
 * signal, no continueAsNew, no search attributes. Scoped out, not silently
 * dropped — see ARCHITECTURE.md D23 for the reasoning.
 */

/** Approval gate for a step, external code resolves it via `DurableDeferred.done`. */
export const ApprovalGate = DurableDeferred.make("ApprovalGate", { success: Schema.Void })

export const DeploymentWorkflow = Workflow.make("DeploymentWorkflow", {
  payload: {
    service: Identifier,
    version: Identifier,
    previousVersion: Schema.optionalKey(Identifier),
    steps: Schema.NonEmptyArray(CanaryStep),
    rules: Schema.NonEmptyArray(MetricRule)
  },
  success: Succeeded,
  // Where D8's split shows up differently: Temporal folds every outcome
  // (including RolledBack) into one success-shaped Result because its
  // workflow return channel is where `flux history`/CLI reads it, and its
  // saga is hand-rolled compensations run from a catch block. Here
  // `Workflow.withCompensation` is wired to the *typed error* channel, so a
  // breach genuinely has to be a failure (`RolledBack` as the error schema)
  // for the built-in compensation to fire at all.
  //
  // MetricsUnavailable/RouterUnavailable are in the union too — the compiler
  // rejected the handler until they were declared, having noticed that
  // `monitorStepActivity`/`shiftTrafficActivity` can fail with them. Temporal
  // has no equivalent static check: an undeclared ActivityFailure just
  // surfaces at runtime as an unhandled workflow exception.
  error: Schema.Union([HealthCheckFailed, RolledBack, MetricsUnavailable, RouterUnavailable]),
  idempotencyKey: (payload) => `${payload.service}-${payload.version}`
})

const POLL_INTERVAL = Duration.millis(1)

export const DeploymentWorkflowLive = DeploymentWorkflow.toLayer(
  Effect.fn(function*(payload) {
    yield* healthCheckActivity({ service: payload.service, version: payload.version })

    let currentPercent = 0

    for (const [index, step] of payload.steps.entries()) {
      const previousVersion = payload.previousVersion
      yield* shiftTrafficActivity(`shiftTraffic-${index}`, {
        service: payload.service,
        version: payload.version,
        weight: step.percent,
        previousVersion
      }).pipe(
        // Compensation cannot itself carry a typed failure (its signature is
        // `Effect<void, never, R2>`) — if the router rejects the rollback
        // shift, `Effect.orDie` turns it into a defect. Temporal's equivalent
        // (`RollbackFailed`) is a first-class, paged outcome; here the same
        // failure mode has nowhere typed to go. A genuine cost, not an
        // oversight — noted for the comparison.
        DeploymentWorkflow.withCompensation(() =>
          previousVersion === undefined
            ? Effect.void
            : shiftTrafficActivity(`shiftTraffic-${index}-compensation`, {
              service: payload.service,
              version: previousVersion,
              weight: 100
            }).pipe(Effect.orDie))
      )
      currentPercent = step.percent

      const evaluation = yield* monitorStepActivity(`monitorStep-${index}`, {
        service: payload.service,
        version: payload.version,
        window: step.monitorDuration,
        pollInterval: POLL_INTERVAL,
        rules: payload.rules
      })

      if (evaluation._tag === "Breached") {
        return yield* Effect.fail(RolledBack.make({
          service: payload.service,
          fromVersion: payload.version,
          toVersion: previousVersion ?? payload.version,
          atPercent: currentPercent,
          breaches: evaluation.breaches
        }))
      }

      if (step.requiresApproval) {
        yield* DurableDeferred.await(ApprovalGate)
      }
    }

    return Succeeded.make({ service: payload.service, version: payload.version })
  })
)
