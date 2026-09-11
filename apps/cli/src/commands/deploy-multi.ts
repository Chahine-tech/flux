import { Console, Effect, Schema } from "effect"
import { compileRolloutPlan } from "@flux/domain"
import { Command, Flag } from "effect/unstable/cli"
import { TriggerMultiRequest } from "@flux/contracts"
import { readFileSync } from "node:fs"
import { clientLayer, makeClient } from "../control-plane.ts"

/**
 * `flux deploy-multi` — roll one version out across several services at once
 *, as a parent workflow over one child per service. The rollout is
 * described by a JSON file (too much for flags); it is validated against the
 * shared contract before being sent to the control plane.
 */
const describe = (failure: { readonly _tag: string } & Record<string, unknown>): string => {
  switch (failure._tag) {
    case "Cycle":
      return `dependency cycle: ${(failure.path as ReadonlyArray<string>).join(" -> ")}`
    case "UnknownDependency":
      return `"${failure.service}" depends on "${failure.dependsOn}", which is not in this rollout`
    default:
      return `"${failure.service}" depends on itself`
  }
}

export const deployMulti = Command.make("deploy-multi", {
  config: Flag.String("config").pipe(
    Flag.withDescription(
      "Path to a JSON file: { services: [...], maxConcurrency, failFast, dependsOn?, onFailure? }"
    )
  ),
  controlPlane: Flag.String("control-plane").pipe(
    Flag.withDefault("http://localhost:8080"),
    Flag.withDescription("Control plane base URL")
  ),
  plan: Flag.Boolean("plan").pipe(
    Flag.withDescription("Print the resolved rollout order and exit, without deploying")
  )
}, (config) =>
  Effect.gen(function*() {
    const raw = yield* Effect.try(() => JSON.parse(readFileSync(config.config, "utf8")))
    const request = yield* Schema.decodeUnknownEffect(TriggerMultiRequest)(raw)

    // `--plan` resolves the dependency graph locally and stops. The control
    // plane compiles it again for real before starting anything, so this is a
    // preview and never the thing the rollout trusts.
    if (config.plan) {
      const compiled = compileRolloutPlan(
        request.services.map((service) => service.service),
        request.dependsOn
      )
      if (compiled._tag !== "Compiled") {
        return yield* Console.error(`[flux] ${describe(compiled)}`)
      }
      yield* Console.log(`[flux] rollout order for ${request.services.length} services:`)
      for (const [index, wave] of compiled.plan.waves.entries()) {
        yield* Console.log(`  ${index + 1}. ${wave.join(", ")}`)
      }
      return
    }

    const client = yield* makeClient(config.controlPlane)
    const { workflowId } = yield* client.deployments.triggerMulti({ payload: request })

    yield* Console.log(
      `[flux] started multi-service rollout ${workflowId} — ${request.services.length} services` +
        ` (max ${request.maxConcurrency} at once, fail-fast ${request.failFast})`
    )
  }).pipe(Effect.provide(clientLayer)))
