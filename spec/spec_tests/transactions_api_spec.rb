require 'spec_helper'

describe 'Transactions API' do
  require_wired_tiger

  define_transactions_spec_tests(TRANSACTIONS_API_TESTS)
end
