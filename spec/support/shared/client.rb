shared_context 'shared client' do

  let(:client) { double('client') }
  let(:db) { Mongo::Database.new(client, TEST_DB) }
  let(:collection) { db[TEST_COLL] }
  let(:connection) { double('connection') }
  let(:node) { double('node') }
  let(:read_obj) { double('read_preference') }
  let(:ascending) { 1 }
  let(:descending) { -1 }

  def stub!
    allow(connection).to receive(:send_message).and_return(true)

    allow(node).to receive(:with_connection).and_yield(connection)

    allow(read_obj).to receive(:primary?).and_return(true)
    allow(read_obj).to receive(:secondary?).and_return(false)
    allow(read_obj).to receive(:secondary_preferred?).and_return(false)
    allow(read_obj).to receive(:primary_preferred?).and_return(false)
    allow(read_obj).to receive(:nearest?).and_return(false)
    allow(read_obj).to receive(:tags_set?).and_return(false)

    allow(collection).to receive(:full_namespace) do
      "#{db.name}.#{collection.name}"
    end
    allow(collection).to receive(:client) { client }
    allow(collection).to receive(:read) { read_obj }

    allow(client).to receive(:mongos?).and_return(false)
    allow(client).to receive(:with_node).and_yield(connection)
  end

end
