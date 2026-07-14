import { Layer } from "effect"
import { HttpRouter, HttpServer } from "effect/unstable/http"
import { HttpApiBuilder } from "effect/unstable/httpapi"
import { afterAll, describe, expect, it } from "vitest"
import { CodecApi, CodecHandlers } from "../src/http/codec-server.ts"

/**
 * The Temporal-UI codec server (D21) as a fetch handler: a payload big enough
 * to compress goes through `/codec/encode` and comes back verbatim from
 * `/codec/decode` — the exact protocol the UI speaks (bytes as base64).
 */

const AppLive = HttpApiBuilder.layer(CodecApi).pipe(
  Layer.provide(CodecHandlers),
  Layer.provide(HttpServer.layerServices)
)

const { dispose, handler } = HttpRouter.toWebHandler(AppLive)
afterAll(() => dispose())

const post = (path: string, body: unknown) =>
  handler(
    new Request(`http://localhost${path}`, {
      method: "POST",
      body: JSON.stringify(body),
      headers: { "content-type": "application/json" }
    })
  )

const b64 = (value: string) => Buffer.from(value).toString("base64")

describe("codec server", () => {
  it("encodes a large payload to gzip and decodes it back", async () => {
    const original = {
      metadata: { encoding: b64("json/plain") },
      data: b64(JSON.stringify({ big: "x".repeat(4096) }))
    }

    const encodedRes = await post("/codec/encode", { payloads: [original] })
    expect(encodedRes.status).toBe(200)
    const encoded = await encodedRes.json() as { payloads: [{ metadata: Record<string, string>; data: string }] }
    expect(Buffer.from(encoded.payloads[0].metadata["encoding"]!, "base64").toString()).toBe("binary/gzip")
    expect(encoded.payloads[0].data.length).toBeLessThan(original.data.length)

    const decodedRes = await post("/codec/decode", { payloads: encoded.payloads })
    expect(decodedRes.status).toBe(200)
    const decoded = await decodedRes.json() as { payloads: [{ metadata: Record<string, string>; data: string }] }
    expect(Buffer.from(decoded.payloads[0].metadata["encoding"]!, "base64").toString()).toBe("json/plain")
    expect(decoded.payloads[0].data).toBe(original.data)
  })

  it("passes through payloads it does not own", async () => {
    const foreign = { metadata: { encoding: b64("json/plain") }, data: b64("{}") }
    const res = await post("/codec/decode", { payloads: [foreign] })
    const body = await res.json() as { payloads: [{ data: string }] }
    expect(body.payloads[0].data).toBe(foreign.data)
  })
})
