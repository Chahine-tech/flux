import { Effect } from "effect"
import { type Notification, NotifyPort } from "../ports/notify.ts"

/**
 * Use case: deliver a deployment notification.
 * Thin program against the NotifyPort — driven by the notify activity.
 */
export const notify = Effect.fn("flux.notify")(function*(notification: Notification) {
  yield* Effect.annotateCurrentSpan({ "flux.kind": notification.kind, "flux.service": notification.service })
  const port = yield* NotifyPort
  yield* port.send(notification)
})
