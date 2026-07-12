/**
 * @flux/domain — schemas, tagged errors, pure business rules.
 *
 * Depends only on the `effect` core (never on `effect/unstable/*`).
 * Temporal workflows import only TYPES from here (`import type`) —
 * never values, so the Effect runtime never enters the workflow
 * bundle (ARCHITECTURE.md D6).
 */
export * from "./config.ts"
export * from "./duration.ts"
export * from "./errors.ts"
export * from "./metrics.ts"
export * from "./result.ts"
export * from "./thresholds.ts"
