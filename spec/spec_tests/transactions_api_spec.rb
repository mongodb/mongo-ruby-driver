# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

require 'runners/crud'
require 'runners/transactions'

describe 'Transactions API' do
  require_wired_tiger

  define_transactions_spec_tests(TRANSACTIONS_API_TESTS)
end
