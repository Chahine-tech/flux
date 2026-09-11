import { defineConfig } from "tsdown"

/**
 * Package the control plane for a container image. Workspace packages are
 * bundled (they ship as `.ts`); Temporal stays external for the native
 * core-bridge its client loads.
 *
 * `@effect/sql-sqlite-node` is deliberately *not* external: it has no
 * dependencies of its own and sits on Node's built-in `node:sqlite`, so there
 * is no native module to keep outside the bundle — which is also why the
 * process logs an experimental-feature warning at startup.
 */
export default defineConfig({
  entry: ["src/main.ts"],
  format: ["esm"],
  platform: "node",
  target: "node24",
  outDir: "dist",
  clean: true,
  sourcemap: true,
  deps: {
    alwaysBundle: [/^@flux\//],
    neverBundle: ["@temporalio/client", "@temporalio/common"]
  }
})
