require 'spec_helper'

describe 'Transactions' do
  clean_slate_on_evergreen

  define_transactions_spec_tests(TRANSACTIONS_TESTS)
end
