import { Effect, Layer, Redacted, Schema, Stream } from "effect"
import { AiError, LanguageModel, type Prompt, type Response } from "effect/unstable/ai"
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/unstable/http"

/**
 * A small, text-only Anthropic provider for effect/unstable/ai's LanguageModel
 * port. This adapter is the one place that knows about Anthropic's Messages API;
 * everything above it just talks to the abstract port.
 *
 * It implements the one hook LanguageModel.make needs, generateText: turn the
 * normalized prompt into a POST /v1/messages over an Effect HttpClient, turn the
 * completion back into a single text part. streamText isn't implemented. The only
 * caller (the rollback postmortem) generates once, non-streaming, so a streaming
 * path would just be untested weight. It fails instead of pretending.
 *
 * The API key is Redacted so it stays out of logs and traces. Empty key (the
 * default, when AI_ANTHROPIC_API_KEY isn't set) fails right away without a
 * network call, so the best-effort postmortem quietly no-ops and an operator who
 * never sets a key pays nothing.
 */

/** How to reach the Anthropic Messages API. */
export interface AnthropicOptions {
  readonly apiKey: Redacted.Redacted<string>
  /** Defaults to `claude-opus-4-8`, the current recommended model. */
  readonly model?: string
  /** Overridable so tests can point at a local double. Defaults to the public API. */
  readonly baseUrl?: string
  /** Upper bound on generated tokens. A postmortem is short; 1024 is plenty. */
  readonly maxTokens?: number
}

const ANTHROPIC_VERSION = "2023-06-01"
const DEFAULT_MODEL = "claude-opus-4-8"
const DEFAULT_BASE_URL = "https://api.anthropic.com"
const DEFAULT_MAX_TOKENS = 1024

/**
 * Lenient decoder for the Messages response: we only care about text blocks and
 * tolerate any other block type the model might emit (`thinking`, etc.).
 */
const MessagesResponse = Schema.Struct({
  content: Schema.Array(
    Schema.Struct({
      type: Schema.String,
      text: Schema.optional(Schema.String)
    })
  )
})

/** One Anthropic message on the wire: a role and a flat text body. */
interface WireMessage {
  readonly role: "user" | "assistant"
  readonly content: string
}

/**
 * Fold the normalized prompt into Anthropic's shape. System messages become the
 * top-level `system` string; user/assistant messages keep their role, with their
 * text parts concatenated (this adapter is text-only, so non-text parts — files
 * — are dropped rather than mis-encoded).
 */
const toRequestBody = (
  prompt: Prompt.Prompt,
  model: string,
  maxTokens: number
): { readonly model: string; readonly max_tokens: number; readonly system?: string; readonly messages: ReadonlyArray<WireMessage> } => {
  const systemParts: Array<string> = []
  const messages: Array<WireMessage> = []

  for (const message of prompt.content) {
    if (message.role === "system") {
      systemParts.push(message.content)
      continue
    }
    if (message.role === "user" || message.role === "assistant") {
      let text = ""
      for (const part of message.content) {
        if (part.type === "text") {
          text += part.text
        }
      }
      messages.push({ role: message.role, content: text })
    }
    // Tool messages have no meaning for a single-shot text completion — skip them.
  }

  const system = systemParts.length > 0 ? systemParts.join("\n\n") : undefined
  return { model, max_tokens: maxTokens, ...(system !== undefined ? { system } : {}), messages }
}

/** Any failure below the port — transport, non-2xx, malformed body — is one retryable provider error. */
const providerError = (method: string, description: string): AiError.AiError =>
  new AiError.AiError({
    module: "AnthropicLanguageModel",
    method,
    reason: new AiError.InternalProviderError({ description })
  })

const generateText = (
  client: HttpClient.HttpClient,
  options: AnthropicOptions
) =>
(providerOptions: LanguageModel.ProviderOptions): Effect.Effect<Array<Response.PartEncoded>, AiError.AiError> => {
  const key = Redacted.value(options.apiKey)
  if (key === "") {
    return Effect.fail(providerError("generateText", "ANTHROPIC_API_KEY is not configured"))
  }

  const baseUrl = options.baseUrl ?? DEFAULT_BASE_URL
  const body = toRequestBody(providerOptions.prompt, options.model ?? DEFAULT_MODEL, options.maxTokens ?? DEFAULT_MAX_TOKENS)

  return client
    .execute(
      HttpClientRequest.post(`${baseUrl}/v1/messages`).pipe(
        HttpClientRequest.setHeaders({
          "x-api-key": key,
          "anthropic-version": ANTHROPIC_VERSION
        }),
        HttpClientRequest.bodyJsonUnsafe(body)
      )
    )
    .pipe(
      Effect.flatMap(HttpClientResponse.filterStatusOk),
      Effect.flatMap((response) => response.json),
      Effect.flatMap(Schema.decodeUnknownEffect(MessagesResponse)),
      Effect.map((decoded): Array<Response.PartEncoded> => {
        const text = decoded.content
          .filter((block) => block.type === "text" && block.text !== undefined)
          .map((block) => block.text)
          .join("")
        return [{ type: "text", text }]
      }),
      Effect.catch((error) =>
        Effect.fail(providerError("generateText", error instanceof Error ? error.message : String(error))))
    )
}

/** `streamText` is not supported by this adapter — flux only ever generates a whole completion. */
const unsupportedStream = (): Stream.Stream<Response.StreamPartEncoded, AiError.AiError> =>
  Stream.fail(providerError("streamText", "the flux Anthropic adapter does not implement streaming"))

/**
 * A `LanguageModel` backed by Anthropic's Messages API. Requires an `HttpClient`.
 */
export const layer = (
  options: AnthropicOptions
): Layer.Layer<LanguageModel.LanguageModel, never, HttpClient.HttpClient> =>
  Layer.effect(
    LanguageModel.LanguageModel,
    Effect.gen(function*() {
      const client = yield* HttpClient.HttpClient
      return yield* LanguageModel.make({
        generateText: generateText(client, options),
        streamText: unsupportedStream
      })
    })
  )

/**
 * A disabled `LanguageModel` that fails every request — for runtimes that never
 * do a postmortem (the demo, most tests). It needs no `HttpClient`, so it slots
 * into `AppServices` without dragging the platform layer along.
 */
export const layerDisabled: Layer.Layer<LanguageModel.LanguageModel> = Layer.effect(
  LanguageModel.LanguageModel,
  LanguageModel.make({
    generateText: () => Effect.fail(providerError("generateText", "LanguageModel is disabled in this runtime")),
    streamText: unsupportedStream
  })
)
