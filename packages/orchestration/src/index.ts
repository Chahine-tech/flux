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
export * from "./metrics.ts"
// The Nexus service handler (D25) is deliberately NOT re-exported here: it is
// worker-side code (`@temporalio/nexus`, `nexus-rpc`) constructed at module
// top level, which defeats tree-shaking — through this barrel it ended up
// bundled into the CLI binary (~4x size). Import it from
// `@flux/orchestration/nexus` instead (same pattern as the workflow
// interceptors subpath).
export * from "./payload-codec.ts"
export * from "./tracing/activity-interceptor.ts"
export * from "./tracing/client-interceptor.ts"
export * from "./tracing/traceparent.ts"
