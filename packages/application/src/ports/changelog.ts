import { Context, type Effect } from "effect"
import type { ChangelogUnavailable } from "../errors.ts"

/** One change between two deployed versions: a short id and its message. */
export interface ChangelogEntry {
  readonly id: string
  readonly message: string
}

/**
 * Port: what changed between two versions of a service.
 *
 * This is the input that turns the rollback postmortem (D30) from a paraphrase
 * of metrics the operator already sees into something grounded — the model can
 * correlate the actual code change with the symptom. Implemented by the GitHub
 * compare adapter; a no-op adapter (empty history) stands in when no source is
 * configured, and in the demo/tests.
 */
export class ChangelogPort extends Context.Service<ChangelogPort, {
  readonly between: (
    params: { readonly service: string; readonly fromVersion: string; readonly toVersion: string }
  ) => Effect.Effect<ReadonlyArray<ChangelogEntry>, ChangelogUnavailable>
}>()("ChangelogPort") {}
