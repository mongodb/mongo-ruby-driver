# frozen_string_literal: true

require 'spec_helper'

describe 'Database#cursor_command overload retry' do
  require_topology :replica_set
  min_server_version '4.4'

  let(:subscriber) { Mrss::EventSubscriber.new }

  let(:client_options) { {} }

  let(:client) do
    authorized_client.with(client_options).tap do |client|
      client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
    end
  end

  let(:collection) { authorized_client['cursor_command_overload_spec'] }

  let(:find_started_events) do
    subscriber.started_events.select { |e| e.command_name == 'find' }
  end

  before do
    collection.drop
    collection.insert_many((1..4).map { |i| { _id: i } })
    authorized_client.use(:admin).command(
      configureFailPoint: 'failCommand',
      mode: { times: 1 },
      data: {
        failCommands: %w[find],
        errorCode: 6,
        errorLabels: %w[RetryableError SystemOverloadedError]
      }
    )
  end

  after do
    authorized_client.use(:admin).command(
      configureFailPoint: 'failCommand',
      mode: 'off'
    )
    client.close
  end

  context 'when retryable reads and writes are enabled' do
    # Force both on so the test is deterministic on Evergreen variants that
    # disable retryable reads or writes suite-wide (e.g. no-retry-writes).
    let(:client_options) { { retry_reads: true, retry_writes: true } }

    it 'retries the initial command' do
      cursor = client.database.cursor_command(
        { find: collection.name, batchSize: 2 }
      )
      expect(cursor.to_a.length).to eq(4)
      expect(find_started_events.length).to eq(2)
    end
  end

  context 'when retryable reads are disabled' do
    let(:client_options) { { retry_reads: false } }

    it 'does not retry and raises the overload error' do
      expect do
        client.database.cursor_command({ find: collection.name, batchSize: 2 })
      end.to raise_error(Mongo::Error::OperationFailure) { |e|
        expect(e.label?('SystemOverloadedError')).to be true
      }
      expect(find_started_events.length).to eq(1)
    end
  end

  context 'when retryable writes are disabled' do
    let(:client_options) { { retry_writes: false } }

    it 'does not retry and raises the overload error' do
      expect do
        client.database.cursor_command({ find: collection.name, batchSize: 2 })
      end.to raise_error(Mongo::Error::OperationFailure) { |e|
        expect(e.label?('SystemOverloadedError')).to be true
      }
      expect(find_started_events.length).to eq(1)
    end
  end
end
