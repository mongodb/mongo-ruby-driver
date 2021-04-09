# frozen_string_literal: true
# encoding: utf-8

require 'spec_helper'

require 'runners/crud'
require 'runners/transactions'

describe 'Transactions' do
  require_wired_tiger

  define_transactions_spec_tests(TRANSACTIONS_TESTS)
end
