# frozen_string_literal: true
# encoding: utf-8

require 'spec_helper'

require 'runners/unified'

base = "#{CURRENT_PATH}/spec_tests/data/change_streams_unified"
CHANGE_STREAM_UNIFIED_TESTS = Dir.glob("#{base}/**/*.yml").sort

describe 'Change stream unified spec tests' do
  define_unified_spec_tests(base, CHANGE_STREAM_UNIFIED_TESTS)
end
