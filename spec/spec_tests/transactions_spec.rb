# frozen_string_literal: true
# encoding: utf-8

require 'spec_helper'

require 'runners/crud'
require 'runners/transactions'

describe 'Transactions' do
  # Storage detection fails for serverless instances. However, it is save to
  # assume that a serverless instance uses WiredTiger Storage Engine,
  # so we skip the check.
  require_wired_tiger unless Utils.serverless?

  define_transactions_spec_tests(TRANSACTIONS_TESTS)
end
