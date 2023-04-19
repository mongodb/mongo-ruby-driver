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
    SpecConfig.instance.require_crypt_shared do
      define_unified_spec_tests(base, CLIENT_SIDE_ENCRYPTION_UNIFIED_TESTS)
    end
  end
end
