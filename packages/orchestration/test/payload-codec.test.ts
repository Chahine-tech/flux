import { describe, expect, it } from "vitest"
import { DEFAULT_THRESHOLD_BYTES, makePayloadCodec } from "../src/payload-codec.ts"

/**
 * The compression codec (D21), unit level: threshold behaviour, symmetric
 * round-trip including the original `encoding` metadata, and pass-through of
 * payloads it does not own. The end-to-end proof (a canary whose input is
 * stored gzipped in Temporal's history) lives in the worker integration test.
 */

const encoder = new TextEncoder()
const decoder = new TextDecoder()

const jsonPayload = (value: unknown) => ({
  metadata: { encoding: encoder.encode("json/plain") },
  data: encoder.encode(JSON.stringify(value))
})

const codec = makePayloadCodec()

describe("payload codec", () => {
  it("leaves small payloads untouched", async () => {
    const payload = jsonPayload({ small: true })
    const [encoded] = await codec.encode([payload])
    expect(encoded).toBe(payload)
  })

  it("compresses large payloads and round-trips them, restoring the original encoding", async () => {
    const payload = jsonPayload({ big: "x".repeat(DEFAULT_THRESHOLD_BYTES * 4) })
    const [encoded] = await codec.encode([payload])

    expect(decoder.decode(encoded!.metadata!["encoding"]!)).toBe("binary/gzip")
    expect(encoded!.data!.length).toBeLessThan(payload.data.length)

    const [decoded] = await codec.decode([encoded!])
    expect(decoder.decode(decoded!.metadata!["encoding"]!)).toBe("json/plain")
    expect(decoder.decode(decoded!.data!)).toBe(decoder.decode(payload.data))
    expect(decoded!.metadata!["flux-original-encoding"]).toBeUndefined()
  })

  it("passes through payloads it did not compress (mixed histories)", async () => {
    const payload = jsonPayload({ pre: "codec" })
    const [decoded] = await codec.decode([payload])
    expect(decoded).toBe(payload)
  })

  it("does not double-compress an already gzipped payload", async () => {
    const payload = jsonPayload({ big: "x".repeat(DEFAULT_THRESHOLD_BYTES * 4) })
    const [once] = await codec.encode([payload])
    const [twice] = await codec.encode([once!])
    expect(twice).toBe(once)
  })
})
