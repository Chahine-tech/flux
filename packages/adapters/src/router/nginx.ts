import { Effect, FileSystem, Layer, Ref, Semaphore } from "effect"
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process"
import { RouterPort, RouterUnavailable, type SetTrafficWeightParams } from "@flux/application"

/**
 * nginx routing adapter — implements RouterPort by generating a weighted
 * upstream config and reloading nginx.
 *
 * Design (ARCHITECTURE.md D10, validated 2026-07-12):
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

/**
 * Two-version canary model (validated default): set `version` to `weight` and
 * distribute the remaining `100 - weight` across the other known versions,
 * proportionally to their current weights (evenly if they are all zero).
 * For the common previous+new pair this yields exact percentages.
 */
export const redistribute = (
  versions: Readonly<Record<string, number>>,
  version: string,
  weight: number
): Record<string, number> => {
  const result: Record<string, number> = { [version]: weight }
  const others = Object.keys(versions).filter((v) => v !== version)
  if (others.length === 0) {
    return result
  }
  const remainder = Math.max(0, 100 - weight)
  const currentSum = others.reduce((sum, v) => sum + (versions[v] ?? 0), 0)
  for (const v of others) {
    result[v] = currentSum === 0
      ? remainder / others.length
      : remainder * ((versions[v] ?? 0) / currentSum)
  }
  return result
}

export interface RenderOptions {
  /** Resolve the backend address (`host:port`) for a service/version. */
  readonly address: (service: string, version: string) => string
  /** Name the upstream block for a service (defaults to the service name). */
  readonly upstreamName?: (service: string) => string
}

/** Render the full set of nginx `upstream` blocks. Weight-0 versions omitted. */
export const renderUpstreams = (state: RouterState, options: RenderOptions): string => {
  const blocks: string[] = []
  for (const [service, versions] of Object.entries(state)) {
    const servers = Object.entries(versions)
      .filter(([, weight]) => Math.round(weight) > 0)
      .map(([version, weight]) => `    server ${options.address(service, version)} weight=${Math.round(weight)};`)
    if (servers.length === 0) {
      continue
    }
    const name = options.upstreamName?.(service) ?? service
    blocks.push(`upstream ${name} {\n${servers.join("\n")}\n}`)
  }
  return `${blocks.join("\n\n")}\n`
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
            [params.service]: redistribute(current[params.service] ?? {}, params.version, params.weight)
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

      return { setTrafficWeight: apply }
    })
  )
