shared_context 'shared client' do

  let(:ascending) { 1 }
  let(:descending) { -1 }

  let(:db) { Mongo::Database.new(client, TEST_DB) }

  let(:read_obj)do
    double('read_preference').tap do |read_obj|
      allow(read_obj).to receive(:primary?).and_return(true)
      allow(read_obj).to receive(:secondary?).and_return(false)
      allow(read_obj).to receive(:secondary_preferred?).and_return(false)
      allow(read_obj).to receive(:primary_preferred?).and_return(false)
      allow(read_obj).to receive(:nearest?).and_return(false)
      allow(read_obj).to receive(:tags_set?).and_return(false)
      allow(read_obj).to receive(:mongos).and_return(:mode => 'secondary')
    end
  end

  let(:client) do
    double('client').tap do |client|
      allow(client).to receive(:mongos?).and_return(false)
      allow(client).to receive(:with_node).and_yield(connection)
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
      allow(connection).to receive(:send_message).and_return(true)
    end
  end

  let(:node) do
    double('node').tap do |node|
      allow(node).to receive(:with_connection).and_yield(connection)
    end
  end
end
