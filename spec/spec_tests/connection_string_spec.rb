# frozen_string_literal: true
# encoding: utf-8

require 'lite_spec_helper'

require 'runners/connection_string'

describe 'Connection String' do
  define_connection_string_spec_tests(CONNECTION_STRING_TESTS)
end
