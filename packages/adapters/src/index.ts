/**
 * @flux/adapters — Layer implementations of the ports.
 *
 * prometheus | nginx | caddy | traefik | http-health | slack.
 * The only place (along with the apps) allowed to import
 * `effect/unstable/*` (HttpClient…) — those modules may break on
 * minor releases (ARCHITECTURE.md §9).
 */
export {}
