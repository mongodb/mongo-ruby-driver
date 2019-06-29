require 'spec_helper'

describe 'Transactions' do

  before(:all) do
    # For some reason all of these tests fail in evergreen otherwise
    if SpecConfig.instance.ci?
      ClientRegistry.instance.close_all_clients
    end
  end

  define_transactions_spec_tests(TRANSACTIONS_TESTS)
end
