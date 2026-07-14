import { Effect, Schema } from "effect"
import { HttpApi, HttpApiBuilder, HttpApiEndpoint, HttpApiGroup } from "effect/unstable/httpapi"
import { decodePayload, DEFAULT_THRESHOLD_BYTES, encodePayload } from "@flux/orchestration"
import type { Payload } from "@temporalio/common"

/**
 * Temporal codec server (D21): the Temporal UI cannot read gzipped payloads on
 * its own, so it POSTs them here — `{payloads: […]}` in proto-JSON form (bytes
 * as base64 strings) — and gets the decoded ones back. `/encode` completes the
 * protocol for tooling that writes payloads.
 *
 * A separate `HttpApi` from `FluxApi` on purpose: the UI calls it from the
 * browser, so it must not sit behind the bearer-token middleware (it only
 * ever sees payloads the caller already has). Point the UI at
 * `http://localhost:8080/codec` (Settings → Codec Server); CORS for the UI
 * origin is opened in the server layer.
 */

const JsonPayload = Schema.Struct({
  metadata: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
  data: Schema.optionalKey(Schema.String)
})
type JsonPayload = typeof JsonPayload.Type

const CodecBody = Schema.Struct({ payloads: Schema.Array(JsonPayload) })

const codec = HttpApiGroup.make("codec")
  .add(HttpApiEndpoint.post("decode", "/codec/decode", { payload: CodecBody, success: CodecBody }))
  .add(HttpApiEndpoint.post("encode", "/codec/encode", { payload: CodecBody, success: CodecBody }))

export const CodecApi = HttpApi.make("flux-codec").add(codec)

// --- proto-JSON ↔ binary payload mapping ---

const toBinary = (payload: JsonPayload): Payload => ({
  metadata: payload.metadata === undefined
    ? {}
    : Object.fromEntries(
      Object.entries(payload.metadata).map(([key, value]) => [key, new Uint8Array(Buffer.from(value, "base64"))])
    ),
  data: payload.data === undefined ? new Uint8Array() : new Uint8Array(Buffer.from(payload.data, "base64"))
})

const toJson = (payload: Payload): JsonPayload => ({
  metadata: Object.fromEntries(
    Object.entries(payload.metadata ?? {}).map(([key, value]) => [
      key,
      Buffer.from(value ?? new Uint8Array()).toString("base64")
    ])
  ),
  data: Buffer.from(payload.data ?? new Uint8Array()).toString("base64")
})

const through = (
  transform: (payload: Payload) => Effect.Effect<Payload, Error>
) =>
(body: { readonly payloads: ReadonlyArray<JsonPayload> }) =>
  Effect.forEach(body.payloads, (payload) => transform(toBinary(payload))).pipe(
    Effect.map((payloads) => ({ payloads: payloads.map(toJson) })),
    Effect.orDie // corrupt payload bytes are a caller defect, not a typed error
  )

export const CodecHandlers = HttpApiBuilder.group(CodecApi, "codec", (handlers) =>
  handlers
    .handle("decode", ({ payload }) => through(decodePayload)(payload))
    .handle("encode", ({ payload }) => through((p) => encodePayload(p, DEFAULT_THRESHOLD_BYTES))(payload)))
