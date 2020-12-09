require 'spec_helper'

require 'runners/unified'

base = "#{CURRENT_PATH}/spec_tests/data/unified"
UNIFIED_TESTS = Dir.glob("#{base}/**/*.yml").sort

describe 'Unified spec tests' do
  define_unified_spec_tests(base, UNIFIED_TESTS)
end
