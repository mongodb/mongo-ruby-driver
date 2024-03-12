# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

require 'runners/unified'

base = "#{CURRENT_PATH}/spec_tests/data/client_side_operations_timeout"
CSOT_TESTS = Dir.glob("#{base}/**/*.yml").sort

describe 'CSOT unified spec tests' do
  define_unified_spec_tests(base, CSOT_TESTS)
end
