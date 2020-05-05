require 'spec_helper'

describe 'Bulk writes with retryable errors' do
  require_topology :replica_set
  # 4.0 required for failCommand
  min_server_fcv '4.0'

  let(:subscriber) { EventSubscriber.new }

  let(:client) do
    authorized_client.with(options).tap do |client|
      client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
    end
  end

  before do
    client['test'].drop

    client.use('admin').command(
      configureFailPoint: 'failCommand',
      mode: { times: 1 },
      data: { failCommands: ['insert'], errorCode: 91 }
    )
  end

  let(:bulk_write) do
    Mongo::BulkWrite.new(
      client['test'],
      [
        { insert_one: { _id: 1 } },
        { insert_one: { _id: 2 } },
        { update_one: { filter: { _id: 1 }, update: { text: 'hello world!' } } }
      ],
      {}
    )
  end

  let(:insert_events) do
    subscriber.started_events.select do |event|
      event.command_name == 'insert'
    end
  end

  let(:update_events) do
    subscriber.started_events.select do |event|
      event.command_name == 'update'
    end
  end

  context 'when two operations of the same type are combined' do
    context 'when using modern retries' do
      let(:options) { { retry_writes: true } }

      it 'retries the combined insert operation' do
        bulk_write.execute

        expect(insert_events.length).to eq(2)
        expect(update_events.length).to eq(1)

        # insert operations are combined
        expect(insert_events.all? { |event| event.command['documents'].length == 2 }).to be true
      end
    end

    context 'when using legacy retries' do
      let(:options) { { retry_writes: false } }

      it 'retries the combined insert operation' do
        bulk_write.execute

        expect(insert_events.length).to eq(2)
        expect(update_events.length).to eq(1)

        # insert operations are combined
        expect(insert_events.all? { |event| event.command['documents'].length == 2 }).to be true
      end
    end

    context 'when retry writes are off' do
      let(:options) { { retry_writes: false, max_write_retries: 0 } }

      it 'raises an exception on the combined inserted operation' do
        expect do
          bulk_write.execute
        end.to raise_error(Mongo::Error::OperationFailure)

        expect(insert_events.length).to eq(1)
        expect(update_events.length).to eq(0)
      end
    end
  end

  context 'when two operations of the same type are split' do
    # Test uses doubles for server descriptions, doubles are
    # incompatible with freezing which linting does for descriptions
    skip_if_linting

    before do
      allow_any_instance_of(Mongo::Server::Description).to receive(:max_write_batch_size).and_return(1)
    end

    context 'when using modern retries' do
      let(:options) { { retry_writes: true } }

      it 'retries only the first operation' do
        bulk_write.execute

        expect(insert_events.length).to eq(3)
        expect(update_events.length).to eq(1)

        # insert operations are split
        expect(insert_events.all? { |event| event.command['documents'].length == 1 }).to be true

        ids = insert_events.map { |event| event.command['documents'].first['_id'] }
        # only the first insert is retried
        expect(ids).to eq([1, 1, 2])
      end
    end

    context 'when using legacy retries' do
      let(:options) { { retry_writes: false } }

      it 'retries only the first operation' do
        bulk_write.execute

        expect(insert_events.length).to eq(3)
        expect(update_events.length).to eq(1)

        # insert operations are split
        expect(insert_events.all? { |event| event.command['documents'].length == 1 }).to be true

        ids = insert_events.map { |event| event.command['documents'].first['_id'] }
        # only the first insert is retried
        expect(ids).to eq([1, 1, 2])
      end
    end

    context 'when retry writes are off' do
      let(:options) { { retry_writes: false, max_write_retries: 0 } }

      it 'retries only the first operation' do
        expect do
          bulk_write.execute
        end.to raise_error(Mongo::Error::OperationFailure)

        expect(insert_events.length).to eq(1)
        expect(update_events.length).to eq(0)
      end
    end
  end
end
