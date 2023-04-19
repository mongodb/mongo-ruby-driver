# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

require 'runners/crud'
require 'runners/transactions'

describe 'Transactions' do
  require_wired_tiger

  define_transactions_spec_tests(TRANSACTIONS_TESTS)
end
