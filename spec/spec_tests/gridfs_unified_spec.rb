# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

require 'runners/unified'

base = "#{CURRENT_PATH}/spec_tests/data/gridfs_unified"
GRIDFS_UNIFIED_TESTS = Dir.glob("#{base}/**/*.yml").sort

describe 'GridFS unified spec tests' do
  define_unified_spec_tests(base, GRIDFS_UNIFIED_TESTS)
end
