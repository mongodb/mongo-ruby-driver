# frozen_string_literal: true

require 'spec_helper'
require 'runners/unified'

base = "#{CURRENT_PATH}/spec_tests/data/client_side_operations_timeout"
CSOT_TESTS = Dir.glob("#{base}/**/*.yml").sort

describe 'CSOT unified spec tests' do
  if [ 1, '1', 'yes', 'true' ].include?(ENV['CSOT_SPEC_TESTS'])
    define_unified_spec_tests(base, CSOT_TESTS)
  else
    skip 'CSOT spec tests are disabled. To enable them set env variable CSOT_SPEC_TESTS to 1'
  end
end
