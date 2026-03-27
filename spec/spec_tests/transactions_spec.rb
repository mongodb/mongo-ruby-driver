# frozen_string_literal: true

require 'spec_helper'

require 'runners/crud'
require 'runners/transactions'

describe 'Transactions' do
  define_transactions_spec_tests(TRANSACTIONS_TESTS)
end
