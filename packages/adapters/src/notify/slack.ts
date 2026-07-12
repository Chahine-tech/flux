import { Effect, Layer, Redacted } from "effect"
import { HttpClient, HttpClientRequest } from "effect/unstable/http"
import { type Notification, NotifyFailed, NotifyPort } from "@flux/application"

/**
 * Slack notification adapter — implements NotifyPort by POSTing to an incoming
 * webhook. The webhook URL is `Redacted` so it never leaks into logs/traces
 * (ARCHITECTURE.md §5). Any failure becomes `NotifyFailed` (non-fatal to a
 * deployment). Requires an `HttpClient`.
 */

const ICONS: Record<Notification["kind"], string> = {
  "started": "🚀",
  "step-advanced": "📈",
  "rolled-back": "⚠️",
  "succeeded": "✅"
}

/** Build the Slack message payload for a notification. */
export const slackPayload = (notification: Notification): { readonly text: string } => ({
  text: `${ICONS[notification.kind]} [flux] ${notification.service}: ${notification.message}`
})

export interface SlackOptions {
  readonly webhookUrl: Redacted.Redacted<string>
}

export const layer = (
  options: SlackOptions
): Layer.Layer<NotifyPort, never, HttpClient.HttpClient> =>
  Layer.effect(
    NotifyPort,
    Effect.gen(function*() {
      const client = yield* HttpClient.HttpClient
      const url = Redacted.value(options.webhookUrl)

      return {
        send: (notification: Notification): Effect.Effect<void, NotifyFailed> =>
          client
            .execute(
              HttpClientRequest.post(url).pipe(
                HttpClientRequest.bodyJsonUnsafe(slackPayload(notification))
              )
            )
            .pipe(
              Effect.asVoid,
              Effect.mapError((error) =>
                new NotifyFailed({
                  channel: "slack",
                  reason: error instanceof Error ? error.message : String(error)
                })
              )
            )
      }
    })
  )
