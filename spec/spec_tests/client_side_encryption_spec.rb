require 'spec_helper'

describe 'Client-Side Encryption' do
  require_libmongocrypt
  require_enterprise

  before do
    unless ClusterConfig.instance.single_server?
      skip "SPEC-2147: tests only work on single server"
    end
  end
  define_transactions_spec_tests(CLIENT_SIDE_ENCRYPTION_TESTS)
end
