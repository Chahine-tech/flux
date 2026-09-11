# syntax=docker/dockerfile:1
#
# Debian, not alpine. `@temporalio/worker` loads a native core-bridge shipped
# as a prebuilt glibc binary, so an alpine image fails at `dlopen` — and it
# fails in the *build* stage, because bundling the workflows imports the worker
# package to do it.
#
# The Temporal worker as an image. Two stages: the first builds, the second runs
# only what is needed to run.
#
# Two separate bundles come out of the build, for two different reasons:
#   - dist/main.mjs          the worker process itself, ordinary Node code
#   - dist/workflow-bundle.js the deterministic code the workflow VM loads
# The second cannot be produced at startup here: the SDK would have to resolve
# `@flux/orchestration/workflows` from the workspace, which a packaged image
# does not have. `FLUX_WORKFLOW_BUNDLE` points the worker at it instead.
FROM node:24-slim AS build
WORKDIR /repo

RUN corepack enable

# The whole workspace: the app bundles its `@flux/*` dependencies from source,
# so a per-package copy would not be enough.
COPY . .
RUN pnpm install --frozen-lockfile

RUN pnpm --filter @flux/worker build \
 && pnpm --filter @flux/worker bundle-workflows \
 && pnpm --filter @flux/worker runtime-manifest

FROM node:24-slim AS runtime
WORKDIR /srv
ENV NODE_ENV=production

# Only what the bundler was told to leave out, at the version this build used.
# `pnpm deploy --prod` would reinstall the whole production tree here, shipping
# a second copy of Effect and every `@flux/*` package already compiled into
# main.mjs — which is most of the image. The manifest is derived from tsdown's
# own `neverBundle`, so the two cannot drift.
COPY --from=build /repo/apps/worker/dist/runtime-package.json ./package.json
# The cache has to go in the same layer, or it stays in the image: npm runs
# here rather than being copied from the build stage, so its cache lands in
# this layer and outweighs what the bundling saved.
RUN npm install --omit=dev --no-audit --no-fund \
 && npm cache clean --force \
 # core-bridge ships a prebuilt binary per platform, ~28 MB each, and a Linux
 # image will never load the macOS or Windows ones. Both Linux architectures
 # stay, so the same image definition works on amd64 and arm64.
 && rm -rf node_modules/@temporalio/core-bridge/releases/*-apple-darwin \
           node_modules/@temporalio/core-bridge/releases/*-windows-*

COPY --from=build /repo/apps/worker/dist ./dist

# Bundled ahead of time; without this the worker falls back to resolving the
# workspace, which is not here.
ENV FLUX_WORKFLOW_BUNDLE=/srv/dist/workflow-bundle.js

# The Effect metric registry, scraped by Prometheus.
EXPOSE 9464

# No shell wrapper: the worker must receive SIGTERM directly so Temporal's
# graceful drain runs before the grace period expires (the knob D40 is about).
CMD ["node", "dist/main.mjs"]
