import { Effect } from "effect"
import type { NotifyFailed } from "../errors.ts"
import { type Notification, NotifyPort } from "../ports/notify.ts"

/**
 * Use case: deliver a deployment notification.
 * Thin program against the NotifyPort — driven by the notify activity.
 */
export const notify = (
  notification: Notification
): Effect.Effect<void, NotifyFailed, NotifyPort> =>
  Effect.gen(function*() {
    const port = yield* NotifyPort
    yield* port.send(notification)
  })
