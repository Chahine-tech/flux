import { describe, it } from "@effect/vitest"
import { expect } from "vitest"
import { isHealthyStatus } from "../src/health/http.ts"
import { slackPayload } from "../src/notify/slack.ts"

describe("isHealthyStatus", () => {
  it("accepts 2xx and rejects everything else", () => {
    expect(isHealthyStatus(200)).toBe(true)
    expect(isHealthyStatus(204)).toBe(true)
    expect(isHealthyStatus(301)).toBe(false)
    expect(isHealthyStatus(503)).toBe(false)
  })
})

describe("slackPayload", () => {
  it("prefixes the message with an icon per kind", () => {
    expect(slackPayload({ kind: "succeeded", service: "api", message: "done" }).text).toContain("✅")
    expect(slackPayload({ kind: "rolled-back", service: "api", message: "regression" }).text).toContain("⚠️")
  })

  it("includes the service and message", () => {
    const text = slackPayload({ kind: "started", service: "api", message: "v2.1.0" }).text
    expect(text).toContain("api")
    expect(text).toContain("v2.1.0")
  })
})
