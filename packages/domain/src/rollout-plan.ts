import { Graph } from "effect"

/**
 * Compiling a declared service dependency graph into a rollout plan.
 *
 * The graph lives here, on the Effect side, and **never reaches the workflow**.
 * Two reasons, and the second is the load-bearing one:
 *
 *  1. D6: a workflow bundle carries no Effect, and `Graph` is an `effect`
 *     module.
 *  2. Replay. A plan compiled here is an argument to the workflow, so it is
 *     frozen in the `WorkflowExecutionStarted` event and replays identically
 *     forever. A graph resolved *inside* the workflow would be recomputed at
 *     replay, and editing the dependency config between the original run and a
 *     replay could reorder it — a divergence, or worse, a silently different
 *     order. Same reason the cron window of D28 is evaluated before admission.
 *
 * So the workflow never receives a graph. It receives flat tables it can run
 * with counters and lookups: how many dependencies each service is still
 * waiting on, who to unblock when one succeeds, and who to skip when one fails.
 */

/** `{ dependent: [things it needs first] }`. Absent key means no dependencies. */
export type DependencyDeclaration = Readonly<Record<string, ReadonlyArray<string>>>

/**
 * What the workflow executes. Deliberately all plain records and arrays: it
 * crosses to a Temporal workflow, so it must be JSON and Effect-free.
 */
export interface RolloutPlan {
  /** Every service, in topological order. Ties keep declaration order. */
  readonly order: ReadonlyArray<string>
  /**
   * Direct dependencies per service, normalized so every service has an entry.
   * The workflow gates each service on these, which makes its scheduler three
   * lines instead of a hand-rolled Kahn loop with counters.
   */
  readonly dependsOn: Readonly<Record<string, ReadonlyArray<string>>>
  /** Transitive dependents, to skip in one lookup when a service fails. */
  readonly transitiveDependents: Readonly<Record<string, ReadonlyArray<string>>>
  /**
   * Topological levels. **Display only** — never schedule from these. Waves
   * imply a barrier that the dependencies do not: with `db` and `cache` both
   * in wave 0 and `api` depending only on `db`, a barrier would make `api`
   * wait for `cache` for no reason. The workflow uses the counters above.
   */
  readonly waves: ReadonlyArray<ReadonlyArray<string>>
}

/**
 * Compilation outcome. A plain tagged union rather than a `TaggedError`, the
 * same shape as `evaluateThresholds` and `evaluateWindow`: the domain stays
 * pure and total, and the control plane maps a failure onto the typed HTTP
 * error at its boundary.
 */
export type PlanCompilation =
  | { readonly _tag: "Compiled"; readonly plan: RolloutPlan }
  /** A closed walk: the cycle in order, with its first service repeated at the end. */
  | { readonly _tag: "Cycle"; readonly path: ReadonlyArray<string> }
  | { readonly _tag: "UnknownDependency"; readonly service: string; readonly dependsOn: string }
  | { readonly _tag: "SelfDependency"; readonly service: string }

/**
 * Compile `services` plus a dependency declaration into a plan.
 *
 * Edges point dependency → dependent (`db → api` for `api dependsOn db`), so
 * topological order is deployment order directly.
 *
 * Total: every failure is a returned tag, nothing throws.
 */
export const compileRolloutPlan = (
  services: ReadonlyArray<string>,
  dependsOn: DependencyDeclaration = {}
): PlanCompilation => {
  const known = new Set(services)

  // Reject bad edges before building anything, so diagnostics name the
  // declaration the operator wrote rather than a graph index.
  for (const [service, deps] of Object.entries(dependsOn)) {
    if (!known.has(service)) {
      return { _tag: "UnknownDependency", service, dependsOn: service }
    }
    for (const dep of deps) {
      if (dep === service) {
        return { _tag: "SelfDependency", service }
      }
      if (!known.has(dep)) {
        return { _tag: "UnknownDependency", service, dependsOn: dep }
      }
    }
  }

  const indexOf = new Map<string, Graph.NodeIndex>()
  const graph = Graph.directed<string, null>((mutable) => {
    for (const service of services) {
      indexOf.set(service, Graph.addNode(mutable, service))
    }
    for (const [service, deps] of Object.entries(dependsOn)) {
      for (const dep of deps) {
        Graph.addEdge(mutable, indexOf.get(dep)!, indexOf.get(service)!, null)
      }
    }
  })

  // `findCycle` hands back the actual node path, which is the whole point of
  // reaching for a graph library here: the operator gets "web → db → api → web"
  // instead of "invalid configuration". The path it returns is already a closed
  // walk (the first node repeated at the end), so nothing is appended — an
  // earlier version did, and printed the last service twice.
  const cycle = Graph.findCycle(graph)
  if (cycle._tag === "Some") {
    return { _tag: "Cycle", path: cycle.value.path.map((index) => nameAt(graph, index)) }
  }

  const order = [...Graph.values(Graph.topo()(graph))]

  const directDependents: Record<string, Array<string>> = {}
  const normalizedDeps: Record<string, ReadonlyArray<string>> = {}
  for (const service of services) {
    directDependents[service] = []
    normalizedDeps[service] = []
  }
  for (const [service, deps] of Object.entries(dependsOn)) {
    normalizedDeps[service] = [...deps]
    for (const dep of deps) {
      directDependents[dep]!.push(service)
    }
  }

  // Transitive closure the cheap way: walk the topological order backwards, so
  // every dependent's own closure is already known when we reach it.
  const transitiveDependents: Record<string, Array<string>> = {}
  for (const service of [...order].reverse()) {
    const reached = new Set<string>()
    for (const dependent of directDependents[service]!) {
      reached.add(dependent)
      for (const further of transitiveDependents[dependent] ?? []) {
        reached.add(further)
      }
    }
    // Keep the reported order deterministic and meaningful.
    transitiveDependents[service] = order.filter((name) => reached.has(name))
  }

  return {
    _tag: "Compiled",
    plan: {
      order,
      dependsOn: normalizedDeps,
      transitiveDependents,
      waves: computeWaves(order, dependsOn)
    }
  }
}

const nameAt = (graph: Graph.DirectedGraph<string, null>, index: Graph.NodeIndex): string => {
  const node = Graph.getNode(graph, index)
  return node._tag === "Some" ? node.value : String(index)
}

/**
 * Level of a service = 1 + the deepest level among its dependencies. Computed
 * over the topological order, so each dependency is already levelled.
 */
const computeWaves = (
  order: ReadonlyArray<string>,
  dependsOn: DependencyDeclaration
): ReadonlyArray<ReadonlyArray<string>> => {
  const level: Record<string, number> = {}
  let deepest = 0
  for (const service of order) {
    const deps = dependsOn[service] ?? []
    const own = deps.length === 0 ? 0 : Math.max(...deps.map((dep) => level[dep]! + 1))
    level[service] = own
    deepest = Math.max(deepest, own)
  }
  return Array.from({ length: deepest + 1 }, (_, wave) => order.filter((service) => level[service] === wave))
}
