require 'spec_helper'

describe do
  define_crud_spec_tests('Retryable writes spec tests', RETRYABLE_WRITES_TESTS.sort) do |spec, req, test|
    let(:client) do
      authorized_client_with_retry_writes
    end
  end
end
