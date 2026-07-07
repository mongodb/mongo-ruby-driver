# frozen_string_literal: true

require 'spec_helper'

require 'runners/unified'

base = "#{CURRENT_PATH}/spec_tests/data/causal_consistency_unified"
CAUSAL_CONSISTENCY_UNIFIED_TESTS = Dir.glob("#{base}/**/*.yml").sort

describe 'Causal consistency unified spec tests' do
  define_unified_spec_tests(base, CAUSAL_CONSISTENCY_UNIFIED_TESTS)
end
