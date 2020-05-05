require 'spec_helper'

describe 'Bulk writes with retryable errors' do
  require_topology :replica_set, :sharded

  let(:subscriber) { EventSubscriber.new }

  let(:client) do
    new_local_client(
      SpecConfig.instance.addresses,
      SpecConfig.instance.test_options.merge(retry_writes: true)
    ).tap do |client|
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

  it 'retries the combined insert operation' do
    bulk_write.execute

    expect(insert_events.length).to eq(2)
    expect(update_events.length).to eq(1)

    # insert operations are combined
    expect(insert_events.all? { |event| event.command['documents'].length == 2 }).to be true
  end

  context 'when two operations of the same type are split' do
    before do
      allow_any_instance_of(Mongo::Server::Description).to receive(:max_write_batch_size).and_return(1)
    end

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
end
