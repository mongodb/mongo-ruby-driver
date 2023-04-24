# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

require 'runners/connection_string'

describe 'Connection String' do
  define_connection_string_spec_tests(CONNECTION_STRING_TESTS)
end
