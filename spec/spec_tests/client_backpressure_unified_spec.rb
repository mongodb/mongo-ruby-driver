# frozen_string_literal: true

require 'spec_helper'

require 'runners/unified'

base = "#{CURRENT_PATH}/spec_tests/data/client_backpressure"
CLIENT_BACKPRESSURE_UNIFIED_TESTS = Dir.glob("#{base}/**/*.yml").sort

describe 'Client backpressure unified spec tests' do
  define_unified_spec_tests(base, CLIENT_BACKPRESSURE_UNIFIED_TESTS)

  around do |example|
    if example.full_description.include?('clientBulkWrite')
      skip 'RUBY-2964: client-level bulk write is not yet implemented'
    end
    example.run
  end
end
