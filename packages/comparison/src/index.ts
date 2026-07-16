/**
 * @flux/comparison — the N7 comparison experiment (D23).
 *
 * The Temporal canary reimplemented on `effect/unstable/workflow` +
 * `unstable/cluster`. Never imported by the production apps — this package
 * exists to produce the comparison ARCHITECTURE.md §8 promised, not to
 * migrate flux.
 */
export * from "./activities.ts"
export * from "./workflow.ts"
