# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

require 'runners/unified'

base = "#{CURRENT_PATH}/spec_tests/data/collection_management"
COLLECTION_MANAGEMENT_TESTS = Dir.glob("#{base}/**/*.yml").sort

describe 'Collection management spec tests' do
  define_unified_spec_tests(base, COLLECTION_MANAGEMENT_TESTS)
end
