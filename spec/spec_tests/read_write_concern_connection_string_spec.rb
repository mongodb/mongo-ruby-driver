# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

require 'runners/connection_string'

READ_WRITE_CONCERN_CONNECTION_STRING_TESTS =
  Dir.glob("#{CURRENT_PATH}/spec_tests/data/read_write_concern/connection-string/*.yml").sort

describe 'Connection String' do
  define_connection_string_spec_tests(READ_WRITE_CONCERN_CONNECTION_STRING_TESTS)
end
