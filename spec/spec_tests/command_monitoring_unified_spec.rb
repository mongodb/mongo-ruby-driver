# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

require 'runners/unified'

base = "#{CURRENT_PATH}/spec_tests/data/command_monitoring_unified"
COMMAND_MONITORING_UNIFIED_TESTS = Dir.glob("#{base}/**/*.yml").sort

describe 'Command monitoring unified spec tests' do
  define_unified_spec_tests(base, COMMAND_MONITORING_UNIFIED_TESTS)
end
