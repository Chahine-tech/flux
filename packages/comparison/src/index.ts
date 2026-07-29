/**
 * @flux/comparison — the comparison experiment.
 *
 * The Temporal canary reimplemented on `effect/unstable/workflow` +
 * `unstable/cluster`. Never imported by the production apps — this package
 * exists to produce the comparison the project set out to write, not to
 * migrate flux.
 */
export * from "./activities.ts"
export * from "./workflow.ts"
