# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

require 'runners/unified'

base = "#{CURRENT_PATH}/spec_tests/data/unified"
PASS_UNIFIED_TESTS = Dir.glob("#{base}/valid-pass/**/*.yml").sort
FAIL_UNIFIED_TESTS = Dir.glob("#{base}/valid-fail/**/*.yml").sort

describe 'Unified spec tests - valid pass' do
  define_unified_spec_tests(base, PASS_UNIFIED_TESTS)
end

describe 'Unified spec tests - expected failures' do
  define_unified_spec_tests(base, FAIL_UNIFIED_TESTS, expect_failure: true)
end
