#!/bin/bash
# Prepares the expansions needed by perf-submission.sh.
#
# The Signal Processing Service needs to know whether a set of results comes
# from the master waterfall (and therefore belongs in the time series used for
# change point detection) or from a patch build.
#
# Writes perf-expansion.yml, which the caller must load with expansions.update.
# A dedicated file is used rather than the shared expansion.yml so that this
# does not have to append to the PREPARE_SHELL block scalar written there.

set -eu

# These are Evergreen expansions, which only reach this script if it is run
# with subprocess.exec and include_expansions_in_env. shell.exec does not put
# them in the environment, so check explicitly rather than failing with a bare
# "unbound variable".
for var in requester revision_order_id; do
  eval "value=\${$var:-}"
  if [ -z "$value" ]; then
    echo "Error: expansion '$var' is missing from the environment." >&2
    echo "Run this script with subprocess.exec and include_expansions_in_env." >&2
    exit 1
  fi
done

out=perf-expansion.yml
: > "$out"

# shellcheck disable=SC2154
if [ "${requester}" = "commit" ]; then
  echo "is_mainline: true" >> "$out"
else
  echo "is_mainline: false" >> "$out"
fi

# revision_order_id looks like "<username>_<order>" for patches and "<order>"
# for mainline commits. SPS wants just the order number.
# shellcheck disable=SC2154
echo "parsed_order_id: $(echo "${revision_order_id}" | awk -F'_' '{print $NF}')" >> "$out"

cat "$out"
