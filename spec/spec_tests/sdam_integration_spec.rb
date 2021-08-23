# frozen_string_literal: true
# encoding: utf-8

require 'spec_helper'

require 'runners/crud'
require 'runners/transactions'

SDAM_INTEGRATION_TESTS = Dir.glob("#{CURRENT_PATH}/spec_tests/data/sdam_integration/*.yml").sort

describe 'SDAM integration tests' do
  require_no_multi_mongos
  require_wired_tiger

  define_transactions_spec_tests(SDAM_INTEGRATION_TESTS)
end
