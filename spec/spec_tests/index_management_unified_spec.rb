# frozen_string_literal: true

require 'spec_helper'
require 'runners/unified'

base = "#{CURRENT_PATH}/spec_tests/data/index_management"
INDEX_MANAGEMENT_UNIFIED_TESTS = Dir.glob("#{base}/**/*.yml").sort

# rubocop:disable RSpec/EmptyExampleGroup
describe 'index management unified spec tests' do
  define_unified_spec_tests(base, INDEX_MANAGEMENT_UNIFIED_TESTS)
end
# rubocop:enable RSpec/EmptyExampleGroup
