require 'spec_helper'

describe 'Client-Side Encryption' do
  require_libmongocrypt
  require_enterprise

  define_transactions_spec_tests(CLIENT_SIDE_ENCRYPTION_TESTS)
end
