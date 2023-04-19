# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

require 'runners/server_selection'

MAX_STALENESS_TESTS = Dir.glob("#{CURRENT_PATH}/spec_tests/data/max_staleness/**/*.yml").sort

describe 'Max staleness spec tests' do
  define_server_selection_spec_tests(MAX_STALENESS_TESTS)
end
