# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

require 'runners/crud'
require 'runners/transactions'

test_paths = Dir.glob("#{CURRENT_PATH}/spec_tests/data/read_write_concern/operation/**/*.yml").sort

describe 'Read write concern operation spec tests' do
  define_transactions_spec_tests(test_paths)
end
