require 'spec_helper'

require 'runners/crud'

describe 'Retryable reads spec tests' do
  require_wired_tiger
  require_no_multi_shard

  define_crud_spec_tests(RETRYABLE_READS_TESTS) do |spec, req, test|
    let(:client) do
      authorized_client.with({max_read_retries: 0}.update(test.client_options)).tap do |client|
        client.subscribe(Mongo::Monitoring::COMMAND, event_subscriber)
      end
    end
  end
end

describe 'Retryable reads spec tests - legacy' do
  require_no_multi_shard

  define_crud_spec_tests(RETRYABLE_READS_TESTS) do |spec, req, test|
    let(:client_options) do
      {
        max_read_retries: 1,
        read_retry_interval: 0,
        retry_reads: false,
      }.update(test.client_options)
    end

    let(:client) do
      authorized_client.with(client_options).tap do |client|
        client.subscribe(Mongo::Monitoring::COMMAND, event_subscriber)
      end
    end

    around do |example|
      desc = example.full_description
      # Skip tests that disable modern retryable reads because they expect
      # no retries - and since legacy retryable reads are used, the tests
      # will fail.
      if desc =~/retryReads is false|fails on first attempt/
        skip 'Test not applicable to legacy read retries'
      end
      example.run
    end
  end
end
