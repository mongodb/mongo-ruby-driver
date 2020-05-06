require 'spec_helper'

require_relative './shared/supports_modern_retries'

describe 'Retryable writes' do
  let(:client) do
    authorized_client.with(
      write: write_concern,
      socket_timeout: socket_timeout,
    )
  end

  let(:write_concern) { nil }
  let(:socket_timeout) { nil }

  let(:collection) { client['test'] }

  before do
    collection.drop
  end

  context '#insert_one' do
    let(:command_name) { 'insert' }

    let(:perform_operation) do
      collection.insert_one(_id: 1)
    end

    let(:actual_result) do
      collection.count(_id: 1)
    end

    let(:successful_result) do
      1
    end

    it_behaves_like 'it supports modern retries'
  end
end
