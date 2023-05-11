# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

require 'runners/unified'

base = "#{CURRENT_PATH}/spec_tests/data/sdam_unified"
SDAM_UNIFIED_TESTS = Dir.glob("#{base}/**/*.yml").sort

describe 'SDAM unified spec tests' do
  define_unified_spec_tests(base, SDAM_UNIFIED_TESTS)
end
