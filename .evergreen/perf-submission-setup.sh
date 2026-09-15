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
