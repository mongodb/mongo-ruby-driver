# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

require 'runners/unified'

base = "#{CURRENT_PATH}/spec_tests/data/crud_unified"
CRUD_UNIFIED_TESTS = Dir.glob("#{base}/**/*.yml").sort

describe 'CRUD unified spec tests' do
  define_unified_spec_tests(base, CRUD_UNIFIED_TESTS)
end
