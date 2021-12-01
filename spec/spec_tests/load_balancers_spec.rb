# frozen_string_literal: true
# encoding: utf-8

require 'spec_helper'

require 'runners/unified'

base = "#{CURRENT_PATH}/spec_tests/data/load_balancers"
LOAD_BALANCER_TESTS = Dir.glob("#{base}/**/*.yml").sort

describe 'Load balancer spec tests' do
  require_topology :load_balanced

  define_unified_spec_tests(base, LOAD_BALANCER_TESTS)
end
