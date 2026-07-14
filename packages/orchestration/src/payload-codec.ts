import { gunzip, gzip } from "node:zlib"
import type { Payload, PayloadCodec } from "@temporalio/common"
import { Effect } from "effect"

/**
 * Payload compression codec (D21) — the byte-level Effect ⇄ Temporal meeting
 * point deferred by D8b. Runs on the main thread (never inside the workflow
 * VM): payloads above `threshold` are gzip-compressed before hitting the wire
 * and Temporal's history, and transparently decompressed on the way back.
 *
 * The codec must be wired **symmetrically** into every process that touches
 * payloads (worker, control plane, CLI) via `dataConverter.payloadCodecs`.
 * Decoding is defensive: a payload without the gzip marker passes through
 * untouched, so mixed histories (written before the codec existed) still read.
 *
 * Each direction is an Effect program executed at the Promise boundary — the
 * same bridge shape as the activities, one layer lower.
 */

/** Payloads smaller than this stay uncompressed — gzip would only add overhead. */
export const DEFAULT_THRESHOLD_BYTES = 1024

const ENCODING_KEY = "encoding"
const GZIP_ENCODING = new TextEncoder().encode("binary/gzip")
/** Where the payload's original `encoding` metadata survives compression. */
const ORIGINAL_ENCODING_KEY = "flux-original-encoding"

const gzipBytes = (data: Uint8Array): Effect.Effect<Uint8Array, Error> =>
  Effect.callback((resume) => {
    gzip(data, (error, compressed) =>
      resume(error === null ? Effect.succeed(new Uint8Array(compressed)) : Effect.fail(error)))
  })

const gunzipBytes = (data: Uint8Array): Effect.Effect<Uint8Array, Error> =>
  Effect.callback((resume) => {
    gunzip(data, (error, plain) =>
      resume(error === null ? Effect.succeed(new Uint8Array(plain)) : Effect.fail(error)))
  })

const isGzipped = (payload: Payload): boolean => {
  const encoding = payload.metadata?.[ENCODING_KEY]
  if (encoding === undefined || encoding === null || encoding.length !== GZIP_ENCODING.length) {
    return false
  }
  return GZIP_ENCODING.every((byte, index) => encoding[index] === byte)
}

/** Compress one payload when it is worth it; smaller ones pass through. */
export const encodePayload = (payload: Payload, threshold: number): Effect.Effect<Payload, Error> => {
  const data = payload.data
  if (data === undefined || data === null || data.length < threshold || isGzipped(payload)) {
    return Effect.succeed(payload)
  }
  return gzipBytes(data).pipe(
    Effect.map((compressed): Payload => ({
      metadata: {
        ...payload.metadata,
        [ENCODING_KEY]: GZIP_ENCODING,
        // Preserve the inner encoding (e.g. json/plain) for the decode side.
        ...(payload.metadata?.[ENCODING_KEY] != null
          ? { [ORIGINAL_ENCODING_KEY]: payload.metadata[ENCODING_KEY] }
          : {})
      },
      data: compressed
    }))
  )
}

/** Undo {@link encodePayload}; foreign payloads pass through untouched. */
export const decodePayload = (payload: Payload): Effect.Effect<Payload, Error> => {
  if (!isGzipped(payload) || payload.data === undefined || payload.data === null) {
    return Effect.succeed(payload)
  }
  return gunzipBytes(payload.data).pipe(
    Effect.map((plain): Payload => {
      const { [ORIGINAL_ENCODING_KEY]: original, ...metadata } = payload.metadata ?? {}
      return {
        metadata: original != null ? { ...metadata, [ENCODING_KEY]: original } : metadata,
        data: plain
      }
    })
  )
}

/** The codec Temporal wires into `dataConverter.payloadCodecs`. */
export const makePayloadCodec = (threshold: number = DEFAULT_THRESHOLD_BYTES): PayloadCodec => ({
  encode: (payloads) =>
    Effect.runPromise(Effect.forEach(payloads, (payload) => encodePayload(payload, threshold))),
  decode: (payloads) => Effect.runPromise(Effect.forEach(payloads, decodePayload))
})
