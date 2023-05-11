# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

require 'runners/unified'

base = "#{CURRENT_PATH}/spec_tests/data/versioned_api"
UNIFIED_TESTS = Dir.glob("#{base}/**/*.yml").sort

describe 'Versioned API spec tests' do
  define_unified_spec_tests(base, UNIFIED_TESTS)
end
