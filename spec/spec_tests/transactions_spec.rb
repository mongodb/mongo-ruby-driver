require 'spec_helper'

describe 'Transactions' do
  require_wired_tiger

  define_transactions_spec_tests(TRANSACTIONS_TESTS)
end
