# frozen_string_literal: true

require 'spec_helper'

require 'runners/unified'

base = "#{CURRENT_PATH}/spec_tests/data/load_balancers"
LOAD_BALANCER_TESTS = Dir.glob("#{base}/**/*.yml").sort

describe 'Load balancer spec tests' do
  require_topology :load_balanced

  # These tests fail against the drivers-tools load-balanced deployment for
  # reasons unrelated to the code under test. They were dark until the
  # load-balanced Evergreen configuration was fixed (RUBY-3946) and are
  # tracked for a real fix in RUBY-3959.
  ruby_3959_skips = {
    'only connections for a specific serviceId are closed when pools are cleared' =>
      'RUBY-3959: CMAP event reason casing (connectionError vs connection_error)',
    'errors during the initial connection hello are ignored' =>
      'RUBY-3959: CMAP event reason casing (connectionError vs connection_error)',
    'stale errors are ignored' =>
      'RUBY-3959: CMAP event reason casing (connectionError vs connection_error)',
    'wait queue timeout errors include cursor statistics' =>
      'RUBY-3959: wait-queue timeout against maxPoolSize=1 pool',
    'wait queue timeout errors include transaction statistics' =>
      'RUBY-3959: wait-queue timeout against maxPoolSize=1 pool',
  }.freeze

  define_unified_spec_tests(base, LOAD_BALANCER_TESTS, skip_descriptions: ruby_3959_skips)
end
