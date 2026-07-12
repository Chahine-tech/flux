/**
 * @flux/orchestration — Temporal workflows + activities.
 *
 * Two sealed worlds (ARCHITECTURE.md D6/D7):
 * - `src/workflows/`: plain TypeScript, deterministic, ZERO `effect`
 *   imports (even transitive — only `import type` from @flux/domain).
 * - `src/activities/`: Effect → Promise bridge via the worker's ManagedRuntime.
 *
 * Also here: interceptors, Schema-based payload codec (D8), sinks.
 */
export {}
