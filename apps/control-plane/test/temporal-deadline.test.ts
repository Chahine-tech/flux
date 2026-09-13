import { Effect } from "effect"
import type { Client } from "@temporalio/client"
import { describe, expect, it } from "vitest"
import { make } from "../src/temporal-client.ts"

/**
 * Every call to Temporal goes through a deadline.
 *
 * D52 injected a Temporal outage into a cluster and the prediction that failed
 * was the interesting one: the 503 D49 designed never arrived, because no call
 * ever *failed*. Nothing returned and nothing was logged. The SDK's own types
 * had said why: "It is strongly recommended to explicitly set deadlines. If no
 * deadline is set, then it is possible for the client to end up waiting forever
 * for a response."
 *
 * What is pinned here is the wiring and the classification: every port method
 * routes through `withDeadline`, and the `DEADLINE_EXCEEDED` that raises is
 * classified as `TemporalUnavailable` rather than escaping as something else.
 * That gRPC really cancels the request is the SDK's contract, not this test's.
 */

/** Never settles, which is what an unreachable cluster looked like. */
const hang = <A>(): Promise<A> => new Promise<A>(() => {})

const deadlineExceeded = () => Object.assign(new Error("14 DEADLINE_EXCEEDED"), { code: 4 })

const fakeClient = (calls: Array<string>): Client => {
  const withDeadline = <A>(deadline: number | Date, fn: () => Promise<A>): Promise<A> => {
    calls.push("withDeadline")
    const ms = Math.max(0, (deadline instanceof Date ? deadline.getTime() : deadline) - Date.now())
    return Promise.race([
      fn(),
      new Promise<A>((_, reject) => setTimeout(() => reject(deadlineExceeded()), ms))
    ])
  }
  const handle = {
    query: hang,
    executeUpdate: hang,
    describe: hang,
    delete: hang,
    update: hang
  }
  return {
    connection: {
      withDeadline,
      workflowService: { describeNamespace: hang }
    },
    workflow: {
      start: hang,
      getHandle: () => handle,
      list: () => ({ [Symbol.asyncIterator]: () => ({ next: hang }) })
    },
    schedule: { create: hang, getHandle: () => handle }
  } as unknown as Client
}

describe("a Temporal cluster that never answers", () => {
  // Short budget so the test is fast; the production default is 10 seconds.
  const withShortBudget = async <A>(run: () => Promise<A>): Promise<A> => {
    const previous = process.env.TEMPORAL_CALL_TIMEOUT_MS
    process.env.TEMPORAL_CALL_TIMEOUT_MS = "150"
    try {
      return await run()
    } finally {
      if (previous === undefined) delete process.env.TEMPORAL_CALL_TIMEOUT_MS
      else process.env.TEMPORAL_CALL_TIMEOUT_MS = previous
    }
  }

  it("fails every call with TemporalUnavailable instead of hanging", async () => {
    await withShortBudget(async () => {
      const calls: Array<string> = []
      const port = make(fakeClient(calls))

      const cases: ReadonlyArray<readonly [string, Effect.Effect<unknown, unknown>]> = [
        ["start", port.start({ service: "api" } as never)],
        ["startMulti", port.startMulti({ services: [] } as never)],
        ["status", port.status("wf1")],
        ["list", port.list(undefined, 10)],
        ["listRunningIds", port.listRunningIds(10)],
        ["listClosed", port.listClosed(10)],
        ["approve", port.approve("wf1")],
        ["abort", port.abort("wf1")],
        ["ensureDriftSchedule", port.ensureDriftSchedule("api", "v2", 60_000)],
        ["disableDrift", port.disableDrift("api")]
      ]

      const started = Date.now()
      for (const [name, effect] of cases) {
        const result = await Effect.runPromise(Effect.result(effect))
        expect(result._tag, `${name} has to fail, not hang`).toBe("Failure")
        if (result._tag !== "Failure") continue
        expect((result.failure as { readonly _tag?: string })._tag, name).toBe("TemporalUnavailable")
      }
      // Ten calls at a 150ms budget each: if any were unbounded this never ends.
      expect(Date.now() - started).toBeLessThan(10_000)
      expect(calls.length, "every call went through withDeadline").toBe(cases.length)
    })
  }, 30_000)

  it("reports what it could not determine rather than guessing", async () => {
    await withShortBudget(async () => {
      const calls: Array<string> = []
      const port = make(fakeClient(calls))

      // These two are total by design, and their answers on a timeout are the
      // safe ones: `unknown` releases no admission slot (D48), and `false`
      // takes the pod out of service without restarting it (D45).
      expect(await Effect.runPromise(port.execution("wf1"))).toBe("unknown")
      expect(await Effect.runPromise(port.reachable)).toBe(false)
      expect(calls.length).toBe(2)
    })
  }, 30_000)
})
