require 'spec_helper'

describe 'Client-Side Encryption' do
  require_libmongocrypt
  require_enterprise
  min_server_fcv '4.2'

  define_transactions_spec_tests(CLIENT_SIDE_ENCRYPTION_TESTS)
end
