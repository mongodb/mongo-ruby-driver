# frozen_string_literal: true
# encoding: utf-8

require 'spec_helper'

require 'runners/crud'
require 'runners/transactions'

describe 'Client-Side Encryption' do
  require_libmongocrypt
  require_enterprise

  define_transactions_spec_tests(CLIENT_SIDE_ENCRYPTION_TESTS, expectations_bson_types: false)
end
