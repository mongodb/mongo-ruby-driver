# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

require 'runners/unified'

base = "#{CURRENT_PATH}/spec_tests/data/retryable_reads"
RETRYABLE_READ_UNIFIED_TESTS = Dir.glob("#{base}/unified/**/*.yml").sort

describe 'Retryable reads spec tests - unified' do
  require_wired_tiger
  require_no_multi_mongos

  define_unified_spec_tests(base, RETRYABLE_READ_UNIFIED_TESTS) do |spec, req, test|
    let(:client) do
      authorized_client.use(spec.database_name).with({max_read_retries: 0}.update(test.client_options)).tap do |client|
        client.subscribe(Mongo::Monitoring::COMMAND, event_subscriber)
      end
    end
  end
end
