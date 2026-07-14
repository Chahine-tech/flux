import { timingSafeEqual } from "node:crypto"
import { Effect, Layer, Option, Redacted } from "effect"
import { Authorization, Unauthorized } from "@flux/contracts"

/**
 * Bearer-token auth for the HTTP API (the `Authorization` middleware declared
 * in the contract). Configured by `API_TOKEN`:
 *
 * - unset → auth is disabled: every request passes (local dev, tests);
 * - set   → every endpoint requires `Authorization: Bearer <token>`, compared
 *   in constant time; anything else is the contract's typed 401.
 *
 * The `/rpc` websocket (read-only watch stream) and `/docs` are not behind the
 * middleware — it guards the `FluxApi` endpoints, which carry all the writes.
 */
const sameToken = (a: string, b: string): boolean => {
  const left = Buffer.from(a)
  const right = Buffer.from(b)
  return left.length === right.length && timingSafeEqual(left, right)
}

export const layer = (token: Option.Option<Redacted.Redacted>): Layer.Layer<Authorization> =>
  Layer.succeed(Authorization, {
    bearer: (handler, { credential }) =>
      Option.match(token, {
        onNone: () => handler,
        onSome: (expected) =>
          sameToken(Redacted.value(credential), Redacted.value(expected))
            ? handler
            : Effect.fail(new Unauthorized({}))
      })
  })
