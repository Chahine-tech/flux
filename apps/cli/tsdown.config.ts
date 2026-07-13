import { defineConfig } from "tsdown"

/**
 * Package the CLI into a distributable binary (N4/D4 matured). The whole
 * monorepo runs from TypeScript source via tsx; here tsdown (rolldown/oxc)
 * produces a real ESM build: the workspace packages (`@flux/*`) and Effect are
 * bundled into one file with a shebang. Only Temporal's client stays external —
 * it loads a native `core-bridge` that cannot be bundled — and is resolved from
 * node_modules at runtime.
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
    // Bundle the workspace source (it ships as .ts, not runnable as-is)…
    alwaysBundle: [/^@flux\//],
    // …but keep Temporal's client external — it loads a native core-bridge.
    neverBundle: ["@temporalio/client"]
  },
  outputOptions: { banner: "#!/usr/bin/env node" }
})
