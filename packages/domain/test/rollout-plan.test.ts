import { describe, expect, it } from "vitest"
import * as FastCheck from "fast-check"
import { compileRolloutPlan } from "../src/rollout-plan.ts"

const compiled = (services: ReadonlyArray<string>, dependsOn = {}) => {
  const result = compileRolloutPlan(services, dependsOn)
  if (result._tag !== "Compiled") throw new Error(`expected Compiled, got ${result._tag}`)
  return result.plan
}

describe("compileRolloutPlan", () => {
  it("puts independent services in one wave, waiting on nothing", () => {
    const plan = compiled(["api", "web", "db"])
    expect(plan.waves).toEqual([["api", "web", "db"]])
    expect(plan.dependsOn).toEqual({ api: [], web: [], db: [] })
    expect(plan.transitiveDependents).toEqual({ api: [], web: [], db: [] })
  })

  it("orders a chain and counts what each service waits on", () => {
    const plan = compiled(["web", "api", "db"], { web: ["api"], api: ["db"] })

    expect(plan.order).toEqual(["db", "api", "web"])
    expect(plan.waves).toEqual([["db"], ["api"], ["web"]])
    expect(plan.dependsOn).toEqual({ db: [], api: ["db"], web: ["api"] })
    // The whole chain downstream of db, not just its direct dependent.
    expect(plan.transitiveDependents.db).toEqual(["api", "web"])
  })

  it("keeps a diamond's two middle services in the same wave", () => {
    const plan = compiled(["web", "api", "cache", "db"], {
      api: ["db"],
      cache: ["db"],
      web: ["api", "cache"]
    })

    expect(plan.waves).toEqual([["db"], ["api", "cache"], ["web"]])
    expect(plan.dependsOn.web).toEqual(["api", "cache"])
    expect(plan.transitiveDependents.db).toEqual(["api", "cache", "web"])
    // api and cache are siblings: neither is downstream of the other.
    expect(plan.transitiveDependents.api).toEqual(["web"])
    expect(plan.transitiveDependents.cache).toEqual(["web"])
  })

  it("names the cycle instead of rejecting the config vaguely", () => {
    const result = compileRolloutPlan(["web", "api", "db"], {
      web: ["api"],
      api: ["db"],
      db: ["web"]
    })

    expect(result._tag).toBe("Cycle")
    if (result._tag !== "Cycle") return
    // A closed walk over the three services: four entries, not five. Pinning
    // the length is what catches an accidentally duplicated tail.
    expect(result.path).toHaveLength(4)
    expect(result.path.at(0)).toBe(result.path.at(-1))
    expect(new Set(result.path.slice(0, -1))).toEqual(new Set(["web", "api", "db"]))
  })

  it("rejects a dependency on a service not in the rollout", () => {
    const result = compileRolloutPlan(["api"], { api: ["db"] })
    expect(result).toEqual({ _tag: "UnknownDependency", service: "api", dependsOn: "db" })
  })

  it("rejects a service depending on itself", () => {
    const result = compileRolloutPlan(["api"], { api: ["api"] })
    expect(result).toEqual({ _tag: "SelfDependency", service: "api" })
  })
})

describe("compileRolloutPlan (property-based)", () => {
  /**
   * Generates a DAG by construction: services are numbered, and an edge may
   * only run from a lower number to a higher one, so a cycle is impossible.
   * Uses fast-check's own runner rather than `it.prop`, same as window.test.ts
   * — the shape being generated is a pair of lists whose validity is a
   * relation between them, which a schema cannot express.
   */
  const dag = FastCheck.integer({ min: 1, max: 8 }).chain((count) => {
    const services = Array.from({ length: count }, (_, i) => `s${i}`)
    const candidates: Array<readonly [string, string]> = []
    for (let dependent = 0; dependent < count; dependent++) {
      for (let dependency = 0; dependency < dependent; dependency++) {
        candidates.push([services[dependent]!, services[dependency]!])
      }
    }
    return FastCheck.subarray(candidates).map((edges) => {
      const dependsOn: Record<string, Array<string>> = {}
      for (const [dependent, dependency] of edges) {
        ;(dependsOn[dependent] ??= []).push(dependency)
      }
      return { services, dependsOn }
    })
  })

  it("always deploys a dependency before the service that needs it", () => {
    FastCheck.assert(
      FastCheck.property(dag, ({ services, dependsOn }) => {
        const plan = compiled(services, dependsOn)

        expect(plan.order).toHaveLength(services.length)

        const waveOf = new Map<string, number>()
        plan.waves.forEach((wave, index) => wave.forEach((service) => waveOf.set(service, index)))

        for (const [service, deps] of Object.entries(dependsOn)) {
          for (const dep of deps) {
            // Earlier in the linear order, and in a strictly earlier wave.
            expect(plan.order.indexOf(dep)).toBeLessThan(plan.order.indexOf(service))
            expect(waveOf.get(dep)!).toBeLessThan(waveOf.get(service)!)
          }
        }
      })
    )
  })

  it("never lists a service as its own dependent", () => {
    FastCheck.assert(
      FastCheck.property(dag, ({ services, dependsOn }) => {
        const plan = compiled(services, dependsOn)
        for (const service of services) {
          expect(plan.transitiveDependents[service]).not.toContain(service)
        }
      })
    )
  })
})
