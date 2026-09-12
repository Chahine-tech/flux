# syntax=docker/dockerfile:1
#
# The control plane as an image. Same shape as the worker's, minus the workflow
# bundle: only the worker loads code into the workflow VM.
#
# Assembles a build made outside. Run first:
#   pnpm --filter @flux/control-plane build
#   pnpm --filter @flux/control-plane runtime-manifest
#
# One replica. The admission controller is an in-memory STM budget and the read
# model is a local SQLite file, so a second replica would give you a second
# budget and a second projection (D40).
FROM node:24-alpine
WORKDIR /srv
ENV NODE_ENV=production

COPY apps/control-plane/dist/runtime-package.json ./package.json
RUN npm install --omit=dev --no-audit --no-fund \
 && npm cache clean --force

COPY apps/control-plane/dist ./dist

# The SQLite read model. A volume in the lab; the file is rebuilt by the
# projector from Temporal if it is lost, so it is a cache, not a source.
ENV READ_MODEL_DB=/srv/data/flux-read-model.db
RUN mkdir -p /srv/data

EXPOSE 8080

# The probes a Kubernetes deployment reads (D45). Liveness ignores Temporal on
# purpose: restarting does not fix a Temporal outage and would discard the
# in-memory admission state. Readiness does depend on it.
HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=3 \
  CMD node -e "fetch('http://localhost:8080/health/ready').then(r => process.exit(r.ok ? 0 : 1), () => process.exit(1))"

CMD ["node", "dist/main.mjs"]
