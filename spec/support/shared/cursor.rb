shared_context 'shared cursor' do

  let(:client) do
    double('client').tap do |client|
      allow(client).to receive(:mongos?).and_return(false)
      allow(client).to receive(:execute).and_return(*get_mores)
    end
  end

  let(:db) { Mongo::Database.new(client, TEST_DB) }
  let(:collection) do
    db[TEST_COLL].tap do |collection|
      allow(collection).to receive(:full_namespace) do
        "#{db.name}.#{collection.name}"
      end
      allow(collection).to receive(:client) { client }
    end
  end

  let(:view_options) { {} }
  let(:view) { Mongo::View::Collection.new(collection, {}, view_options) }

  let(:nonzero) { 1 }
  let(:b) { proc { |d| d } }

  let(:response) { make_response(1, 3) }

  def make_response(cursor_id = 0, nreturned = 5)
    double('response').tap do |response|
      allow(response).to receive(:documents) { (0...nreturned).to_a }
      allow(response).to receive(:cursor_id) { cursor_id }
    end
  end

  def get_mores
    [ make_response ]
  end
end
