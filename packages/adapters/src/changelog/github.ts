import { Effect, Layer, Redacted, Schema } from "effect"
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/unstable/http"
import { type ChangelogEntry, ChangelogPort, ChangelogUnavailable } from "@flux/application"

/**
 * ChangelogPort backed by GitHub's compare API.
 *
 * `GET /repos/{owner}/{repo}/compare/{base}...{head}` returns the commits
 * between two refs; flux's version strings are used as the refs (tags). The
 * result feeds the rollback postmortem so the model can correlate the actual
 * change with the regression instead of guessing.
 *
 * The service→repo mapping is a single template (`repoTemplate`, e.g.
 * `"acme/{service}"`) — a deliberate simplification for a repo-per-service
 * convention. An empty template disables the source; every failure (bad ref,
 * private repo, network) is a `ChangelogUnavailable` the postmortem treats as
 * "no changelog" rather than an error.
 */

const GITHUB_API_VERSION = "2022-11-28"
const DEFAULT_BASE_URL = "https://api.github.com"

export interface GitHubChangelogOptions {
  /** Template mapping a service to `owner/repo`, e.g. `"acme/{service}"`. Empty disables the source. */
  readonly repoTemplate: string
  /** A GitHub token; empty is allowed (public repos, subject to tighter rate limits). */
  readonly token: Redacted.Redacted<string>
  /** Overridable so tests can point at a local double. Defaults to the public API. */
  readonly baseUrl?: string
}

/** Lenient decoder for the compare response — only the commit list and messages. */
const CompareResponse = Schema.Struct({
  commits: Schema.Array(
    Schema.Struct({
      sha: Schema.String,
      commit: Schema.Struct({ message: Schema.String })
    })
  )
})

export const layer = (
  options: GitHubChangelogOptions
): Layer.Layer<ChangelogPort, never, HttpClient.HttpClient> =>
  Layer.effect(
    ChangelogPort,
    Effect.gen(function*() {
      const client = yield* HttpClient.HttpClient
      const baseUrl = options.baseUrl ?? DEFAULT_BASE_URL
      const token = Redacted.value(options.token)

      return {
        between: ({ fromVersion, service, toVersion }) => {
          const unavailable = (reason: string) => new ChangelogUnavailable({ service, reason })
          if (options.repoTemplate === "") {
            return Effect.fail(unavailable("no changelog source configured"))
          }
          const repo = options.repoTemplate.replaceAll("{service}", service)
          const authHeaders = token === "" ? {} : { authorization: `Bearer ${token}` }

          return client
            .execute(
              HttpClientRequest.get(`${baseUrl}/repos/${repo}/compare/${fromVersion}...${toVersion}`).pipe(
                HttpClientRequest.setHeaders({
                  accept: "application/vnd.github+json",
                  "x-github-api-version": GITHUB_API_VERSION,
                  ...authHeaders
                })
              )
            )
            .pipe(
              Effect.flatMap(HttpClientResponse.filterStatusOk),
              Effect.flatMap((response) => response.json),
              Effect.flatMap(Schema.decodeUnknownEffect(CompareResponse)),
              Effect.map((decoded): ReadonlyArray<ChangelogEntry> =>
                decoded.commits.map((commit) => ({
                  id: commit.sha.slice(0, 7),
                  // The first line is the commit subject — the useful signal.
                  message: commit.commit.message.split("\n", 1)[0] ?? commit.commit.message
                }))
              ),
              Effect.catch((error) =>
                Effect.fail(unavailable(error instanceof Error ? error.message : String(error))))
            )
        }
      }
    })
  )

/** A ChangelogPort that always reports no history — for runtimes with no source (demo, tests). */
export const layerNone: Layer.Layer<ChangelogPort> = Layer.succeed(ChangelogPort, {
  between: ({ service }) => Effect.fail(new ChangelogUnavailable({ service, reason: "changelog source disabled" }))
})
