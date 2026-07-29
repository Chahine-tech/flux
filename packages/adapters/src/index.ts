/**
 * @flux/adapters — Layer implementations of the ports.
 *
 * prometheus | nginx | caddy | traefik | http-health | slack |
 * anthropic (LanguageModel).
 * The only place (along with the apps) allowed to import
 * `effect/unstable/*` (HttpClient, ai…) — those modules may break on
 * minor releases, so they stay isolated behind the ports.
 */
export * as AnthropicLanguageModel from "./ai/anthropic-language-model.ts"
export * as GitHubChangelog from "./changelog/github.ts"
export * as HttpHealth from "./health/http.ts"
export * as HttpJsonMetrics from "./metrics/http-json.ts"
export * as PrometheusMetrics from "./metrics/prometheus.ts"
export * as SlackNotify from "./notify/slack.ts"
export * as CaddyRouter from "./router/caddy.ts"
export * as NginxRouter from "./router/nginx.ts"
