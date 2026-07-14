/**
 * The two-version canary weight model shared by every router adapter — how a
 * `setTrafficWeight(version, weight)` call turns into a full weight table.
 */

/**
 * Set `version` to `weight` and distribute the remaining `100 - weight` across
 * the other known versions, proportionally to their current weights (evenly if
 * they are all zero). For the common previous+new pair this yields exact
 * percentages.
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

/**
 * The state to redistribute against. When the router has no state for the
 * service yet (first deployment, or an adapter restarted with no memory of the
 * service) the previous version — when the caller provides it — is seeded at
 * 100%, so the first canary step yields `previous:90 / new:10` instead of a
 * single-upstream table that would send *all* traffic to the canary.
 */
export const baseline = (
  current: Readonly<Record<string, number>>,
  params: { readonly version: string; readonly previousVersion?: string | undefined }
): Readonly<Record<string, number>> =>
  Object.keys(current).length === 0 &&
    params.previousVersion !== undefined &&
    params.previousVersion !== params.version
    ? { [params.previousVersion]: 100 }
    : current
