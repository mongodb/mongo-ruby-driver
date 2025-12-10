# frozen_string_literal: true
# rubocop:todo all

require 'opentelemetry-sdk'

require 'spec_helper'

require 'runners/unified'

base = "#{CURRENT_PATH}/spec_tests/data/open_telemetry"
OTEL_UNIFIED_TESTS = Dir.glob("#{base}/**/*.yml").sort
SKIPPED_OTEL_TESTS = [
  'bulk_write.yml', 'map_reduce.yml', 'atlas_search.yml'
]

TESTS_TO_RUN = OTEL_UNIFIED_TESTS.reject do |path|
  SKIPPED_OTEL_TESTS.include?(File.basename(path))
end

SKIPPED_OTEL_TESTS.each do |filename|
  warn "Skipping OpenTelemetry unified spec test: #{filename}"
end

describe 'CSOT unified spec tests' do
  if [ 1, '1', 'yes', 'true' ].include?(ENV['OTEL_SPEC_TESTS'])
    define_unified_spec_tests(base, TESTS_TO_RUN)
  else
    skip 'OTel spec tests are disabled. To enable them set env variable OTEL_SPEC_TESTS to 1'
  end
end
