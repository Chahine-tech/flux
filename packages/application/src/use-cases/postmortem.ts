import { Effect } from "effect"
import type { AiError } from "effect/unstable/ai"
import { LanguageModel, Prompt } from "effect/unstable/ai"
import type { ThresholdBreach } from "@flux/domain"
import { type ChangelogEntry, ChangelogPort } from "../ports/changelog.ts"

/**
 * Use case: draft a short rollback postmortem with a language model (D30).
 *
 * This is the one place flux uses `effect/unstable/ai`, and it uses it the way
 * the abstraction intends: the program depends on the *abstract* `LanguageModel`
 * service, never on a concrete provider. Which model actually answers — the
 * Anthropic adapter, a disabled stub, a test double — is decided by whatever
 * `Layer` the runtime provides, exactly like the four hand-written ports.
 *
 * The prompt is split the way the port normalizes it: a fixed `system` message
 * setting the role, and a `user` message carrying the breach facts. The provider
 * maps each to its wire equivalent. The caller (the rollback activity) runs this
 * best-effort — a failure here never affects the rollback that already happened.
 */

/** Everything the model needs to reason about a rollback. */
export interface RollbackContext {
  readonly service: string
  readonly version: string
  readonly previousVersion: string
  /** The traffic percentage the canary had reached when a metric breached. */
  readonly atPercent: number
  readonly breaches: ReadonlyArray<ThresholdBreach>
}

const SYSTEM =
  "You are a site reliability engineer. A progressive canary deployment just " +
  "rolled back because a metric crossed its budget. You are given the breached " +
  "metrics and the code changes shipped in the new version. In 2-3 sentences, " +
  "state which metric regressed and by how much, then name the single change " +
  "most likely to explain it (cite the commit). If the changes don't obviously " +
  "explain the regression, say so rather than inventing a cause. Be concrete " +
  "and terse; no preamble, no bullet lists."

/** Render the breach facts and the version's changes into the user turn. */
const renderContext = (ctx: RollbackContext, changes: ReadonlyArray<ChangelogEntry>): string => {
  const breaches = ctx.breaches
    .map((breach) => `- ${breach.metric}: observed ${breach.observed}, budget ${breach.limit}`)
    .join("\n")
  const changelog = changes.length === 0
    ? "(no changelog available)"
    : changes.map((change) => `- ${change.id} ${change.message}`).join("\n")
  return (
    `Service: ${ctx.service}\n` +
    `Rolled back from ${ctx.version} to ${ctx.previousVersion} at ${ctx.atPercent}% traffic.\n` +
    `Breached metrics:\n${breaches}\n` +
    `Changes in ${ctx.version} (since ${ctx.previousVersion}):\n${changelog}`
  )
}

export const postmortem: (
  ctx: RollbackContext
) => Effect.Effect<string, AiError.AiError, LanguageModel.LanguageModel | ChangelogPort> = Effect.fn(
  "flux.postmortem"
)(
  function*(ctx: RollbackContext) {
    yield* Effect.annotateCurrentSpan({ "flux.service": ctx.service, "flux.atPercent": ctx.atPercent })
    // The changelog is grounding, not a prerequisite: if the source is
    // unconfigured or unreachable, fall back to a metrics-only postmortem
    // rather than failing the whole thing.
    const changelogPort = yield* ChangelogPort
    const changelog = yield* changelogPort.between({
      service: ctx.service,
      fromVersion: ctx.previousVersion,
      toVersion: ctx.version
    }).pipe(Effect.catch(() => Effect.succeed<ReadonlyArray<ChangelogEntry>>([])))
    const prompt = Prompt.make([
      { role: "system", content: SYSTEM },
      { role: "user", content: renderContext(ctx, changelog) }
    ])
    const response = yield* LanguageModel.generateText({ prompt, toolChoice: "none" })
    return response.text
  }
)
