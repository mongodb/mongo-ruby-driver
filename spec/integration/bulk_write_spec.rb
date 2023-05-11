# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'Bulk writes' do
  before do
    authorized_collection.drop
  end

  context 'when bulk write is larger than 48MB' do
    let(:operations) do
      [ { insert_one: { text: 'a' * 1000 * 1000 } } ] * 48
    end

    it 'succeeds' do
      expect do
        authorized_collection.bulk_write(operations)
      end.not_to raise_error
    end

    context 'in transaction' do
      require_transaction_support
      min_server_version "4.4"

      it 'succeeds' do
        authorized_collection.create
        expect do
          authorized_collection.client.start_session do |session|
            session.with_transaction do
              authorized_collection.bulk_write(operations, { session: session })
            end
          end
        end.not_to raise_error
      end
    end
  end

  context 'when bulk write needs to be split' do
    let(:subscriber) { Mrss::EventSubscriber.new }

    let(:max_bson_size) { Mongo::Server::ConnectionBase::DEFAULT_MAX_BSON_OBJECT_SIZE }

    let(:insert_events) do
      subscriber.command_started_events('insert')
    end

    let(:failed_events) do
      subscriber.failed_events
    end

    let(:operations) do
      [{ insert_one: { text: 'a' * (max_bson_size/2) } }] * 6
    end

    before do
      authorized_client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
      authorized_collection.bulk_write(operations)
    end

    context '3.6+ server' do
      min_server_fcv '3.6'

      it 'splits the operations' do
        # 3.6+ servers can send multiple bulk operations in one message,
        # with the whole message being limited to 48m.
        expect(insert_events.length).to eq(2)
      end
    end

    context 'pre-3.6 server' do
      max_server_version '3.4'

      it 'splits the operations' do
        # Pre-3.6 servers limit the entire message payload to the size of
        # a single document which is 16m. Given our test data this means
        # twice as many messages are sent.
        expect(insert_events.length).to eq(4)
      end
    end

    it 'does not have a command failed event' do
      expect(failed_events).to be_empty
    end
  end
end
