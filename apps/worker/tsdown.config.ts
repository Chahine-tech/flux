import { defineConfig } from "tsdown"

/**
 * Package the worker for a container image. Same shape as the CLI's build: the
 * workspace packages ship as `.ts` and are not runnable as-is, so they are
 * bundled in; Temporal stays external because `@temporalio/worker` loads a
 * native core-bridge that cannot be bundled, and is installed in the image.
 *
 * This does not bundle the *workflows* — those go through
 * `scripts/bundle-workflows.ts`, which produces the sandbox bundle the SDK
 * loads separately. Two bundles, two reasons: this one is ordinary Node code,
 * that one is deterministic code for the workflow VM.
 */
export default defineConfig({
  entry: ["src/main.ts"],
  format: ["esm"],
  platform: "node",
  target: "node24",
  outDir: "dist",
  // The workflow bundle is written into the same directory and must survive.
  clean: false,
  sourcemap: true,
  deps: {
    alwaysBundle: [/^@flux\//],
    neverBundle: ["@temporalio/worker", "@temporalio/client"]
  }
})
