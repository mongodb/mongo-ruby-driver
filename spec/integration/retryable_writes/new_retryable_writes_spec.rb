require 'spec_helper'

require_relative './shared/supports_modern_retries'
require_relative './shared/supports_legacy_retries'
require_relative './shared/does_not_support_retries'

describe 'Retryable writes' do
  require_fail_command
  require_wired_tiger
  require_no_multi_shard

  let(:client) do
    authorized_client.with(
      socket_timeout: socket_timeout,
      retry_writes: retry_writes,
      max_write_retries: max_write_retries,
    )
  end

  let(:socket_timeout) { nil }
  let(:retry_writes) { nil }
  let(:max_write_retries) { nil }

  let(:collection) { client['test'] }

  before do
    collection.drop
  end

  context 'collection#insert_one' do
    let(:command_name) { 'insert' }

    let(:perform_operation) do
      collection.insert_one(_id: 1)
    end

    let(:actual_result) do
      collection.count(_id: 1)
    end

    let(:expected_successful_result) do
      1
    end

    let(:expected_failed_result) do
      0
    end

    it_behaves_like 'it supports modern retries'
    it_behaves_like 'it supports legacy retries'
  end

  context 'database#command' do
    let(:command_name) { 'ping' }

    let(:perform_operation) do
      collection.database.command(ping: 1)
    end

    it_behaves_like 'it does not support retries'
  end

  # TODO: add more commands to test
end
