# frozen_string_literal: true
# encoding: utf-8

require 'spec_helper'

require 'runners/unified'

base = "#{CURRENT_PATH}/spec_tests/data/client_side_encryption"
CLIENT_SIDE_ENCRYPTION_UNIFIED_TESTS = Dir.glob("#{base}/unified/**/*.yml").sort

describe 'Client side encryption spec tests - unified' do
  require_libmongocrypt
  require_enterprise

  define_unified_spec_tests(base, CLIENT_SIDE_ENCRYPTION_UNIFIED_TESTS)
end
