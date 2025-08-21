# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

require 'runners/unified'

base = "#{CURRENT_PATH}/spec_tests/data/open_telemetry"
OTEL_UNIFIED_TESTS = Dir.glob("#{base}/**/*.yml").sort

describe 'OpenTelemetry unified spec tests' do
  define_unified_spec_tests(base, OTEL_UNIFIED_TESTS)
end
