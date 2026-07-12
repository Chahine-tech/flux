import { Context, type Effect } from "effect"
import type { NotifyFailed } from "../errors.ts"

/** A deployment notification to deliver to an external channel. */
export interface Notification {
  readonly kind: "started" | "step-advanced" | "rolled-back" | "succeeded"
  readonly service: string
  readonly message: string
}

/**
 * Port: deliver notifications (Slack/webhook).
 * Implemented by the Slack adapter; mocked in tests.
 */
export class NotifyPort extends Context.Service<NotifyPort, {
  readonly send: (notification: Notification) => Effect.Effect<void, NotifyFailed>
}>()("NotifyPort") {}
