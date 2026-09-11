import { readFile, writeFile } from "node:fs/promises"
import { fileURLToPath } from "node:url"
import manifest from "../package.json" with { type: "json" }
import config from "../tsdown.config.ts"

/**
 * Emit the package.json the runtime image installs.
 *
 * The rule is the app's own `dependencies` minus whatever tsdown was told to
 * bundle: everything else it externalises, so everything else has to be in the
 * image. An earlier version used `deps.neverBundle` instead and produced an
 * image that died at startup with "cannot find package 'effect'" —
 * `neverBundle` is only the subset tsdown must *never* touch, not the full set
 * it leaves out.
 *
 * The emitted bundle is then read back as a check rather than as the source.
 * Scanning it for specifiers over-collects (bundled code mentions optional
 * packages it never loads, like `node-fetch`), but every *top-level* import has
 * to be installed, so a missing one is a hard error here instead of a container
 * that starts and immediately exits.
 */
const bundled: ReadonlyArray<RegExp | string> =
  (config as { deps?: { alwaysBundle?: ReadonlyArray<RegExp | string> } }).deps?.alwaysBundle ?? []

const isBundled = (name: string): boolean =>
  bundled.some((pattern) => typeof pattern === "string" ? pattern === name : pattern.test(name))

const external = Object.keys(manifest.dependencies ?? {}).filter((name) => !isBundled(name))
if (external.length === 0) {
  throw new Error("every dependency is bundled — the image would have nothing to install")
}

/** `effect/unstable/ai` → `effect`; `@effect/platform-node` stays whole. */
const packageOf = (specifier: string): string => {
  const parts = specifier.split("/")
  return specifier.startsWith("@") ? parts.slice(0, 2).join("/") : parts[0]!
}

const bundlePath = fileURLToPath(new URL("../dist/main.mjs", import.meta.url))
const code = await readFile(bundlePath, "utf8")
const topLevel = new Set<string>()
for (const match of code.matchAll(/^import\s+(?:[^"';]*?from\s*)?["']([^"']+)["']/gm)) {
  const specifier = match[1]!
  if (specifier.startsWith(".") || specifier.startsWith("node:")) continue
  topLevel.add(packageOf(specifier))
}

const missing = [...topLevel].filter((name) => !external.includes(name))
if (missing.length > 0) {
  throw new Error(
    `${bundlePath} imports ${missing.join(", ")} at top level, but the runtime manifest does not install ` +
      `${missing.length > 1 ? "them" : "it"}. The image would fail to start.`
  )
}

const versionOf = async (name: string): Promise<string> => {
  const file = fileURLToPath(new URL(`../node_modules/${name}/package.json`, import.meta.url))
  const { version } = JSON.parse(await readFile(file, "utf8")) as { version: string }
  return version
}

const dependencies = Object.fromEntries(
  await Promise.all(external.sort().map(async (name) => [name, await versionOf(name)] as const))
)

const out = fileURLToPath(new URL("../dist/runtime-package.json", import.meta.url))
await writeFile(out, `${JSON.stringify({ private: true, type: "module", dependencies }, null, 2)}\n`, "utf8")
console.log(`[flux] runtime manifest -> ${out}`, dependencies)
