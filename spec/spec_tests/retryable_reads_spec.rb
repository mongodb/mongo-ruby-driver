require 'spec_helper'

describe 'Retryable reads spec tests' do
  require_no_multi_shard

  define_crud_spec_tests(RETRYABLE_READS_TESTS) do |spec, req, test|
    let(:client) do
      root_authorized_client.with({max_read_retries: 0}.update(test.client_options)).tap do |client|
        client.subscribe(Mongo::Monitoring::COMMAND, event_subscriber)
      end
    end
  end
end
