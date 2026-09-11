import { describe, expect, it } from "vitest"
import { workflowSource } from "../src/worker-config.ts"

/**
 * `workflowSource` decides two coupled things at once: where the workflow code
 * comes from, and where the workflow-side tracing interceptors of D24 go. They
 * cannot be chosen independently — a prebuilt bundle has the interceptors
 * compiled in, while the dev route needs them passed as module paths — and
 * getting it wrong loses the traceparent silently rather than loudly, which is
 * why it is worth a test of its own rather than a comment.
 */
describe("workflowSource", () => {
  it("bundles from the workspace and passes the interceptors as modules, by default", () => {
    const { source, workflowModules } = workflowSource({})

    expect(source).toHaveProperty("workflowsPath")
    expect((source as { workflowsPath: string }).workflowsPath).toMatch(/orchestration.*workflows/)
    expect(workflowModules).toHaveLength(1)
    expect(workflowModules[0]).toMatch(/workflow-interceptors/)
  })

  it("takes a prebuilt bundle and asks for no interceptor modules", () => {
    const { source, workflowModules } = workflowSource({ FLUX_WORKFLOW_BUNDLE: "/srv/workflow-bundle.js" })

    expect(source).toEqual({ workflowBundle: { codePath: "/srv/workflow-bundle.js" } })
    // They are already inside the bundle; a path here would have nothing to
    // resolve against.
    expect(workflowModules).toEqual([])
  })

  it("treats an empty variable as unset rather than as a bundle at path ''", () => {
    const { source } = workflowSource({ FLUX_WORKFLOW_BUNDLE: "" })
    expect(source).toHaveProperty("workflowsPath")
  })
})
