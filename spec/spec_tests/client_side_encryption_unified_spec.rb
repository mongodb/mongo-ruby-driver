# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

require 'runners/unified'

base = "#{CURRENT_PATH}/spec_tests/data/client_side_encryption"
CLIENT_SIDE_ENCRYPTION_UNIFIED_TESTS = Dir.glob("#{base}/unified/**/*.yml").sort

describe 'Client side encryption spec tests - unified' do
  require_libmongocrypt
  require_enterprise

  context 'with mongocryptd' do
    SpecConfig.instance.without_crypt_shared_lib_path do
      define_unified_spec_tests(base, CLIENT_SIDE_ENCRYPTION_UNIFIED_TESTS)
    end
  end

  context 'with crypt_shared' do
    # Only define tests when crypt_shared is available (MONGO_RUBY_DRIVER_CRYPT_SHARED_LIB_PATH
    # is set). On mongocryptd-only configurations (FLE=mongocryptd) crypt_shared is deliberately
    # absent and these tests would fail with "Crypt shared library is required, but cannot be loaded".
    if SpecConfig.instance.crypt_shared_lib_path
      SpecConfig.instance.require_crypt_shared do
        define_unified_spec_tests(base, CLIENT_SIDE_ENCRYPTION_UNIFIED_TESTS)
      end
    end
  end
end
