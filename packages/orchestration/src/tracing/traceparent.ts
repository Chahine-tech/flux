import type { Payload } from "@temporalio/common"

/**
 * W3C `traceparent` encode/decode over a Temporal `Headers` entry (D24). Pure,
 * dependency-free (no `effect` import) — safe to import from the workflow-side
 * interceptor module, which is bundled into the deterministic VM (D6) and must
 * stay Effect-free like the workflow files themselves.
 */

export const TRACEPARENT_HEADER_KEY = "traceparent"

// Flags are hex per W3C (e.g. `01` sampled, but `ff` is valid) — we only ever
// emit `01`, yet parse must accept any spec-compliant value.
const TRACEPARENT_PATTERN = /^00-([0-9a-f]{32})-([0-9a-f]{16})-[0-9a-f]{2}$/

export const formatTraceparent = (traceId: string, spanId: string): string => `00-${traceId}-${spanId}-01`

export const parseTraceparent = (value: string): { readonly traceId: string; readonly spanId: string } | undefined => {
  const match = TRACEPARENT_PATTERN.exec(value)
  return match ? { traceId: match[1]!, spanId: match[2]! } : undefined
}

const encoder = new TextEncoder()
const decoder = new TextDecoder()

export const encodeHeaderPayload = (value: string): Payload => ({
  metadata: { encoding: encoder.encode("json/plain") },
  data: encoder.encode(JSON.stringify(value))
})

export const decodeHeaderPayload = (payload: Payload | undefined): string | undefined => {
  if (payload?.data == null) return undefined
  try {
    return JSON.parse(decoder.decode(payload.data)) as string
  } catch {
    return undefined
  }
}
