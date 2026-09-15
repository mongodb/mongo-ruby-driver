#!/bin/bash
# Submits DriverBench results to the Signal Processing Service (SPS), which
# stores the time series and runs change point detection over it.
#
# This replaces Evergreen's perf.send command, which is deprecated and no
# longer maintained. See:
#   https://docs.devprod.prod.corp.mongodb.com/performance/getting_started/migrating_from_perfSend_to_sps
#
# Expects the expansions written by perf-submission-setup.sh to be in the
# environment (add_expansions_to_env), plus PERFORMANCE_RESULTS_FILE.

set -eu

# See the note in perf-submission-setup.sh: these are Evergreen expansions and
# only reach this script via include_expansions_in_env.
for var in project_id version_id build_variant parsed_order_id task_name task_id execution is_mainline; do
  eval "value=\${$var:-}"
  if [ -z "$value" ]; then
    echo "Error: expansion '$var' is missing from the environment." >&2
    echo "Run this script with subprocess.exec and include_expansions_in_env." >&2
    exit 1
  fi
done

results_file="${PERFORMANCE_RESULTS_FILE:-perf.json}"

if [ ! -f "$results_file" ]; then
  echo "Error: results file '$results_file' does not exist" >&2
  exit 1
fi

endpoint="https://performance-monitoring-api.corp.mongodb.com/raw_perf_results/cedar_report"

# shellcheck disable=SC2154
response=$(curl -s -w "\nHTTP_STATUS:%{http_code}" -X 'POST' \
  "${endpoint}?project=${project_id}&version=${version_id}&variant=${build_variant}&order=${parsed_order_id}&task_name=${task_name}&task_id=${task_id}&execution=${execution}&mainline=${is_mainline}" \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d @"$results_file")

http_status=$(echo "$response" | grep "HTTP_STATUS" | awk -F':' '{print $2}')
response_body=$(echo "$response" | sed '/HTTP_STATUS/d')

echo "Response Body: $response_body"
echo "HTTP Status: $http_status"

# Fail the task if the data was not accepted. A silently dropped submission
# looks exactly like a passing benchmark run, so this must be loud.
if [ "$http_status" -ne 200 ]; then
  echo "Error: performance data was not submitted (HTTP $http_status)" >&2
  exit 1
fi
