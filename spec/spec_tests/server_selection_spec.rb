# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

require 'runners/server_selection'

SERVER_SELECTION_TESTS = Dir.glob("#{CURRENT_PATH}/spec_tests/data/server_selection/**/*.yml").sort

describe 'Server selection spec tests' do
  define_server_selection_spec_tests(SERVER_SELECTION_TESTS)
end
