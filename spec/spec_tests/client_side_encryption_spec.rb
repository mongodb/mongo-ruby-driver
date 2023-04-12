# frozen_string_literal: true
# encoding: utf-8

require 'spec_helper'

require 'runners/crud'
require 'runners/transactions'

describe 'Client-Side Encryption' do
  require_libmongocrypt
  require_enterprise

  context 'with mongocryptd' do
    SpecConfig.instance.without_crypt_shared_lib_path do
      define_transactions_spec_tests(CLIENT_SIDE_ENCRYPTION_TESTS, expectations_bson_types: true)
    end
  end

  context 'with crypt_shared' do
    # Under JRuby+Evergreen, these specs complain about the crypt_shared
    # library not loading; however, crypt_shared appears to load for other
    # specs that require it (see the client_side_encryption_unified_spec and
    # mongocryptd_prose_spec tests).
    fails_on_jruby

    SpecConfig.instance.require_crypt_shared do
      define_transactions_spec_tests(CLIENT_SIDE_ENCRYPTION_TESTS, expectations_bson_types: true)
    end
  end
end
