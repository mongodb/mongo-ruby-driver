require 'spec_helper'

require 'runners/crud'

describe 'Retryable writes spec tests' do
  require_wired_tiger
  require_no_multi_shard

  define_crud_spec_tests(RETRYABLE_WRITES_TESTS) do |spec, req, test|
    let(:client) do
      # Evergreen allows a RETRY_WRITES environment variable to be specified,
      # which determines whether authorized_client has retry_writes enabled by default.
      # The line below ensures that the retryable_write spec tests still work,
      # even if retryable writes was initially disabled for this client, by
      # enabling retry_writes by default, which is the standard driver behavior.
      authorized_client.with({ retry_writes: true }.merge(test.client_options))
    end
  end
end
