# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

require 'runners/unified'

base = "#{CURRENT_PATH}/spec_tests/data/run_command_unified"
RUN_COMMAND_UNIFIED_TESTS = Dir.glob("#{base}/**/*.yml").sort

describe 'runCommand unified spec tests' do
  define_unified_spec_tests(base, RUN_COMMAND_UNIFIED_TESTS)
end
