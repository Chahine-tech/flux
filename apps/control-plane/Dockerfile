# syntax=docker/dockerfile:1
#
# The control plane as an image. Same two-stage shape as the worker, minus the
# workflow bundle — only the worker loads code into the workflow VM.
#
# One replica. The admission controller is an in-memory STM budget and the read
# model is a local SQLite file, so a second replica would give you a second
# budget and a second projection (D40).
FROM node:24-alpine AS build
WORKDIR /repo

RUN corepack enable

COPY . .
RUN pnpm install --frozen-lockfile

RUN pnpm --filter @flux/control-plane build \
 && pnpm --filter @flux/control-plane runtime-manifest

FROM node:24-alpine AS runtime
WORKDIR /srv
ENV NODE_ENV=production

# Only the externals tsdown left out; everything else is inside main.mjs.
COPY --from=build /repo/apps/control-plane/dist/runtime-package.json ./package.json
# The cache has to go in the same layer, or it stays in the image: npm runs
# here rather than being copied from the build stage, so its cache lands in
# this layer and outweighs what the bundling saved.
RUN npm install --omit=dev --no-audit --no-fund \
 && npm cache clean --force

COPY --from=build /repo/apps/control-plane/dist ./dist

# The SQLite read model. A volume in the lab; the file is rebuilt by the
# projector from Temporal if it is lost, so it is a cache, not a source.
ENV READ_MODEL_DB=/srv/data/flux-read-model.db
RUN mkdir -p /srv/data

EXPOSE 8080

CMD ["node", "dist/main.mjs"]
