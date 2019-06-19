require 'spec_helper'

describe 'Retryable writes spec tests' do
  require_no_multi_shard

  define_crud_spec_tests(RETRYABLE_WRITES_TESTS) do |spec, req, test|
    let(:client) do
      authorized_client_with_retry_writes
    end
  end
end
