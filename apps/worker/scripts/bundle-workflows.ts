import { writeFile } from "node:fs/promises"
import { dirname } from "node:path"
import { fileURLToPath } from "node:url"
import { mkdir } from "node:fs/promises"
import { bundleWorkflowCode } from "@temporalio/worker"

/**
 * Pre-bundle the workflows for a container image.
 *
 * In dev the worker passes `workflowsPath` and the SDK runs webpack at startup,
 * resolving `@flux/orchestration/workflows` from the workspace. That resolution
 * does not survive a packaged image, so the bundle is built here instead and the
 * worker is handed a path (`FLUX_WORKFLOW_BUNDLE`). It is also what a real
 * Temporal deployment does: bundling once at build time rather than on every
 * worker boot.
 *
 * The tracing interceptors have to go **in** the bundle. They are workflow-side
 * code (D24), so passing them as `interceptors.workflowModules` at runtime only
 * works on the `workflowsPath` route — with a prebuilt bundle the SDK has
 * nowhere to resolve them from. `workflowInterceptorModules` is where they
 * belong, and getting this wrong loses the traceparent silently rather than
 * loudly.
 */
const out = fileURLToPath(new URL("../dist/workflow-bundle.js", import.meta.url))

const { code } = await bundleWorkflowCode({
  workflowsPath: fileURLToPath(import.meta.resolve("@flux/orchestration/workflows")),
  workflowInterceptorModules: [
    fileURLToPath(import.meta.resolve("@flux/orchestration/tracing/workflow-interceptors"))
  ]
})

await mkdir(dirname(out), { recursive: true })
await writeFile(out, code, "utf8")
console.log(`[flux] workflow bundle -> ${out} (${(code.length / 1024 / 1024).toFixed(2)} MiB)`)
