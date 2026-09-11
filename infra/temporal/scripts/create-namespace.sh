#!/bin/sh
#
# Registers the `default` namespace, then exits.
#
# The other half of what `temporalio/auto-setup` used to do. This stays infra
# rather than moving into the worker next to `ensureSearchAttributes`, because
# the two are not the same kind of thing: the search attributes are flux's own
# schema and meaningless to anyone else, while creating a namespace picks a
# retention period and is an operator's call. flux pointed at a real cluster
# should register its attributes and never create namespaces.
#
# Idempotent: `describe` first, `create` only if absent, so a re-up is a no-op.
#
# The compose already gates this on the server's healthcheck, but that probe is
# only a TCP connect — it says the frontend accepts connections, not that the
# cluster will accept writes. Hence the bounded retry.
set -eu

NAMESPACE=${DEFAULT_NAMESPACE:-default}
ADDRESS=${TEMPORAL_ADDRESS:-temporal:7233}
MAX_ATTEMPTS=${MAX_ATTEMPTS:-30}
SLEEP_SECONDS=${SLEEP_SECONDS:-2}

attempt=1
while :; do
  if temporal operator namespace describe -n "${NAMESPACE}" --address "${ADDRESS}" >/dev/null 2>&1; then
    echo "[flux] namespace '${NAMESPACE}' already exists"
    exit 0
  fi

  # Last attempt runs unsilenced and replaces this shell, so the real error
  # reaches the log and becomes the container's exit code.
  if [ "${attempt}" -ge "${MAX_ATTEMPTS}" ]; then
    echo "[flux] '${NAMESPACE}' still missing after ${MAX_ATTEMPTS} attempts, last error follows" >&2
    exec temporal operator namespace create -n "${NAMESPACE}" --address "${ADDRESS}"
  fi

  if temporal operator namespace create -n "${NAMESPACE}" --address "${ADDRESS}" >/dev/null 2>&1; then
    echo "[flux] namespace '${NAMESPACE}' created"
    exit 0
  fi

  echo "[flux] cluster not ready for namespace writes, retrying (${attempt}/${MAX_ATTEMPTS})"
  attempt=$((attempt + 1))
  sleep "${SLEEP_SECONDS}"
done
