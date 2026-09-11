#!/bin/sh
#
# Creates and migrates Temporal's two PostgreSQL databases, then exits.
#
# This is the half of the retired `temporalio/auto-setup` image that flux still
# needs: `temporal-sql-tool` is a Go binary that only exists inside the
# admin-tools image, and it has to run before the server starts, so no flux
# process is alive to do this instead. The compose gates the server on this
# container exiting 0 (`service_completed_successfully`).
#
# `update-schema` with no explicit version applies every migration bundled in
# the image, which keeps this in lockstep with the image tag instead of a
# version string someone has to remember to bump: admin-tools 1.31.2 carries
# core v1.19 and visibility v1.14, which is what server 1.31 requires.
#
# Idempotent: `create` on an existing database and `update-schema` already at
# the current version are both no-ops, so re-running against the persisted
# pgdata volume does nothing.
set -eu

# The tool reads the password from SQL_PASSWORD itself; host, port, user and
# database are passed as flags below. Guard all four, or a missing one shows up
# as an opaque connection error several seconds later.
: "${POSTGRES_SEEDS:?POSTGRES_SEEDS is required}"
: "${POSTGRES_USER:?POSTGRES_USER is required}"
: "${SQL_PASSWORD:?SQL_PASSWORD is required (temporal-sql-tool reads it from the environment)}"
DB_PORT=${DB_PORT:-5432}

echo "[flux] waiting for PostgreSQL at ${POSTGRES_SEEDS}:${DB_PORT}"
nc -z -w 10 "${POSTGRES_SEEDS}" "${DB_PORT}"

# `-p` is the port, not the password — the password flag is `--pw`.
sql_tool() {
  local db=$1
  shift
  temporal-sql-tool \
    --plugin postgres12 \
    --ep "${POSTGRES_SEEDS}" \
    -p "${DB_PORT}" \
    -u "${POSTGRES_USER}" \
    --db "${db}" \
    "$@"
}

setup_db() {
  local db=$1
  local schema_dir=$2
  echo "[flux] setting up ${db}"
  sql_tool "${db}" create
  sql_tool "${db}" setup-schema -v 0.0
  sql_tool "${db}" update-schema -d "${schema_dir}"
}

setup_db temporal /etc/temporal/schema/postgresql/v12/temporal/versioned
# Advanced visibility: flux's `history` command runs search-attribute queries,
# which is why this deployment is on PostgreSQL rather than SQLite.
setup_db temporal_visibility /etc/temporal/schema/postgresql/v12/visibility/versioned

echo "[flux] PostgreSQL schema ready"
