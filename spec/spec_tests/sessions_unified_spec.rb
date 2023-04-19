# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

require 'runners/unified'

base = "#{CURRENT_PATH}/spec_tests/data/sessions_unified"
SESSIONS_UNIFIED_TESTS = Dir.glob("#{base}/**/*.yml").sort

describe 'Sessions unified spec tests' do
  define_unified_spec_tests(base, SESSIONS_UNIFIED_TESTS)
end
