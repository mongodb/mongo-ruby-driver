# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

require 'runners/crud'

base = "#{CURRENT_PATH}/spec_tests/data/retryable_writes"
RETRYABLE_WRITES_TESTS = Dir.glob("#{base}/legacy/**/*.yml").sort

describe 'Retryable writes spec tests - legacy' do
  require_wired_tiger
  require_no_multi_mongos

  # Do not run these tests when write retries are disabled globally -
  # the tests won't work in that case and testing them with retries enabled
  # is simply redundant.
  require_retry_writes

  define_crud_spec_tests(RETRYABLE_WRITES_TESTS) do |spec, req, test|
    let(:client) do
      authorized_client.with(test.client_options.merge({max_write_retries: 0}))
    end
  end
end
