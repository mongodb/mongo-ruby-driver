# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

require 'runners/unified'

base = "#{CURRENT_PATH}/spec_tests/data/transactions_unified"
# See https://jira.mongodb.org/browse/RUBY-3502 for more details
TRANSACTIONS_UNIFIED_TESTS = Dir.glob("#{base}/**/*.yml").sort.reject { |name| name =~ /.*mongos-unpin.yml$/ }

describe 'Transactions unified spec tests' do
  define_unified_spec_tests(base, TRANSACTIONS_UNIFIED_TESTS)
end
