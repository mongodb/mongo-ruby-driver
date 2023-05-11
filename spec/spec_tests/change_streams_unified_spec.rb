# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

require 'runners/unified'

base = "#{CURRENT_PATH}/spec_tests/data/change_streams_unified"
CHANGE_STREAM_UNIFIED_TESTS = Dir.glob("#{base}/**/*.yml").sort

describe 'Change stream unified spec tests' do
  require_no_multi_mongos
  define_unified_spec_tests(base, CHANGE_STREAM_UNIFIED_TESTS)
end
