import { Effect, FileSystem, Layer, Ref, Semaphore } from "effect"
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process"
import { RouterPort, RouterUnavailable, type SetTrafficWeightParams, type VersionWeight } from "@flux/application"
import { baseline, redistribute } from "./weights.ts"

/**
 * nginx routing adapter — implements RouterPort by generating a weighted
 * upstream config and reloading nginx.
 *
 * Design:
 * - Weight registry: a plain `Ref` (service → version → weight). A single map
 *   with no multi-cell atomic coordination — STM/TxRef would buy nothing here.
 *   STM is reserved for the multi-service coordinator (N4), where several
 *   transactional cells must commit atomically with retry and no I/O inside.
 * - The critical section (regenerate config → write file → reload) is
 *   serialized with a `Semaphore(1)`: the write + reload are irreversible I/O
 *   and must not sit inside an STM transaction (which retries optimistically
 *   and could fire them multiple times — the canonical STM constraint).
 * - Reload runs a *configurable* command through `unstable/process`, so the
 *   same adapter serves nginx / a custom shell hook / etc.
 *
 * Requires `FileSystem` and `ChildProcessSpawner` in context (Node layers
 * provided by the composition root).
 */

/** service → (version → weight). */
export type RouterState = Readonly<Record<string, Readonly<Record<string, number>>>>

// The two-version canary weight model lives in weights.ts (shared by every
// router adapter); re-exported for existing importers.
export { redistribute }

export interface RenderOptions {
  /** Resolve the backend address (`host:port`) for a service/version. */
  readonly address: (service: string, version: string) => string
  /** Name the upstream block for a service (defaults to the service name). */
  readonly upstreamName?: (service: string) => string
}

/**
 * Render the full set of nginx `upstream` blocks. Weight-0 versions omitted.
 * Each server line carries a `# flux-version=<version>` marker so {@link parseServiceState}
 * can read the routing back without reversing the (arbitrary) address function.
 */
export const renderUpstreams = (state: RouterState, options: RenderOptions): string => {
  const blocks: string[] = []
  for (const [service, versions] of Object.entries(state)) {
    const servers = Object.entries(versions)
      .filter(([, weight]) => Math.round(weight) > 0)
      .map(([version, weight]) =>
        `    server ${options.address(service, version)} weight=${Math.round(weight)}; # flux-version=${version}`
      )
    if (servers.length === 0) {
      continue
    }
    const name = options.upstreamName?.(service) ?? service
    blocks.push(`upstream ${name} {\n${servers.join("\n")}\n}`)
  }
  return `${blocks.join("\n\n")}\n`
}

const escapeRegExp = (value: string): string => value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")

/** Parse the weights actually written for one upstream block (reverse of {@link renderUpstreams}). */
export const parseServiceState = (config: string, upstreamName: string): Array<VersionWeight> => {
  const block = new RegExp(`upstream\\s+${escapeRegExp(upstreamName)}\\s*\\{([^}]*)\\}`).exec(config)
  if (block === null) {
    return []
  }
  const line = /weight=(\d+);\s*#\s*flux-version=(\S+)/g
  const result: Array<VersionWeight> = []
  let match: RegExpExecArray | null
  while ((match = line.exec(block[1]!)) !== null) {
    result.push({ version: match[2]!, weight: Number(match[1]) })
  }
  return result
}

export interface NginxOptions extends RenderOptions {
  /** Path the generated upstream config is written to. */
  readonly configPath: string
  /** Reload command, e.g. `["nginx", "-s", "reload"]`. */
  readonly reloadCommand: readonly [string, ...ReadonlyArray<string>]
}

export const layer = (
  options: NginxOptions
): Layer.Layer<RouterPort, never, FileSystem.FileSystem | ChildProcessSpawner.ChildProcessSpawner> =>
  Layer.effect(
    RouterPort,
    Effect.gen(function*() {
      const fs = yield* FileSystem.FileSystem
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
      const state = yield* Ref.make<RouterState>({})
      const lock = yield* Semaphore.make(1)

      const [reloadCmd, ...reloadArgs] = options.reloadCommand

      const apply = (params: SetTrafficWeightParams): Effect.Effect<void, RouterUnavailable> =>
        Effect.gen(function*() {
          const next = yield* Ref.updateAndGet(state, (current) => ({
            ...current,
            // `baseline` seeds the previous version at 100% when the registry has
            // no state for the service (first deployment, or a worker restarted
            // mid-canary) — without it the first shift would render a single
            // upstream and send all traffic to the canary.
            [params.service]: redistribute(
              baseline(current[params.service] ?? {}, params),
              params.version,
              params.weight
            )
          }))
          yield* fs.writeFileString(options.configPath, renderUpstreams(next, options))
          const exitCode = yield* spawner.exitCode(ChildProcess.make(reloadCmd, reloadArgs))
          if (Number(exitCode) !== 0) {
            return yield* Effect.fail(
              new RouterUnavailable({ service: params.service, reason: `reload exited with ${exitCode}` })
            )
          }
        }).pipe(
          Effect.mapError((error) =>
            error instanceof RouterUnavailable
              ? error
              : new RouterUnavailable({
                service: params.service,
                reason: error instanceof Error ? error.message : String(error)
              })
          ),
          lock.withPermits(1)
        )

      const readState = (service: string): Effect.Effect<ReadonlyArray<VersionWeight>, RouterUnavailable> =>
        Effect.gen(function*() {
          const name = options.upstreamName?.(service) ?? service
          if (!(yield* fs.exists(options.configPath))) {
            return []
          }
          const content = yield* fs.readFileString(options.configPath)
          return parseServiceState(content, name)
        }).pipe(
          Effect.mapError((error) =>
            new RouterUnavailable({
              service,
              reason: error instanceof Error ? error.message : String(error)
            })
          )
        )

      return { setTrafficWeight: apply, readState }
    })
  )
