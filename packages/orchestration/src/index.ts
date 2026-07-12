/**
 * @flux/orchestration — Temporal workflows + activities.
 *
 * Two sealed worlds:
 * - `src/workflows/` : plain TypeScript, deterministic, ZERO `effect`
 *   imports. Bundled by Temporal via the `./workflows` entry point — NOT
 *   re-exported here, so nothing accidentally pulls it into an Effect context.
 * - `src/activities/` : Effect → Promise bridge via the worker's ManagedRuntime.
 */
export * from "./activities/index.ts"
export * from "./deployment-input.ts"
export * from "./mapper.ts"
