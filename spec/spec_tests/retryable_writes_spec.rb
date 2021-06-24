# frozen_string_literal: true
# encoding: utf-8

require 'spec_helper'

require 'runners/crud'

describe 'Retryable writes spec tests' do
  unless Utils.serverless?
    # Storage detection fails for serverless instances. However, it is safe to
    # assume that a serverless instance uses WiredTiger Storage Engine,
    # so we skip the check.
    require_wired_tiger
  end
  require_no_multi_shard

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
