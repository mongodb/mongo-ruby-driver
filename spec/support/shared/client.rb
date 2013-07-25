shared_context 'shared client' do

  let(:client) { double('client') }
  let(:db) { Mongo::Database.new(client, TEST_DB) }
  let(:collection) { db[TEST_COLL] }
  let(:connection) { double('connection') }
  let(:node) { double('node') }
  let(:read) { :primary }
  let(:ascending) { 1 }
  let(:descending) { -1 }

  def stub!
    connection.stub(:send_message) { true }

    node.stub(:with_connection).and_yield(connection)

    collection.stub(:full_namespace) { "#{db.name}.#{collection.name}" }
    collection.stub(:read) { read }

    client.stub(:mongos?) { false }
    client.stub(:with_node).and_yield(connection)
    client.stub(:secondary_preferred?) { false }
    client.stub(:secondary?) { false }
    client.stub(:primary?) { true }
  end

end
