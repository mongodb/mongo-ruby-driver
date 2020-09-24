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
  end

  context 'when bulk write needs to be split' do
    let(:subscriber) { EventSubscriber.new }

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

    it 'splits the operations' do
      expect(insert_events.length).to eq(2)
    end

    it 'does not have a command failed event' do
      expect(failed_events).to be_empty
    end
  end
end
