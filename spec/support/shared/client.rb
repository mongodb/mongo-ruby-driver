shared_context 'shared client' do

  let(:ascending) { 1 }
  let(:descending) { -1 }

  let(:db) { Mongo::Database.new(client, TEST_DB) }

  let(:read_obj) { Mongo::ServerPreference.get(:primary) }

  let(:client) do
    double('client').tap do |client|
      allow(client).to receive(:mongos?) { false }
      allow(client).to receive(:op_timeout) { nil }
      allow(client).to receive(:with_context).and_yield(connection)
    end
  end

  let(:collection) do
    db[TEST_COLL].tap do |collection|
      allow(collection).to receive(:full_namespace) do
        "#{db.name}.#{collection.name}"
      end
      allow(collection).to receive(:client) { client }
      allow(collection).to receive(:read) { read_obj }
    end
  end

  let(:connection) do
    double('connection').tap do |connection|
      allow(connection).to receive(:send_message) { true }
    end
  end

  let(:server) do
    double('server').tap do |server|
      allow(server).to receive(:with_connection).and_yield(connection)
    end
  end
end
