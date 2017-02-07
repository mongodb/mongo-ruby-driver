require 'spec_helper'

describe Mongo::Database do

  describe '#==' do

    let(:database) do
      described_class.new(authorized_client, TEST_DB)
    end

    context 'when the names are the same' do

      let(:other) do
        described_class.new(authorized_client, TEST_DB)
      end

      it 'returns true' do
        expect(database).to eq(other)
      end
    end

    context 'when the names are not the same' do

      let(:other) do
        described_class.new(authorized_client, :other)
      end

      it 'returns false' do
        expect(database).to_not eq(other)
      end
    end

    context 'when the object is not a database' do

      it 'returns false' do
        expect(database).to_not eq('test')
      end
    end
  end

  describe '#[]' do

    let(:database) do
      described_class.new(authorized_client, TEST_DB)
    end

    context 'when providing a valid name' do

      let(:collection) do
        database[:users]
      end

      it 'returns a new collection' do
        expect(collection.name).to eq('users')
      end
    end

    context 'when providing an invalid name' do

      it 'raises an error' do
        expect do
          database[nil]
        end.to raise_error(Mongo::Error::InvalidCollectionName)
      end
    end

    context 'when the client has options' do

      let(:client) do
        Mongo::Client.new([default_address.host], TEST_OPTIONS.merge(read: { mode: :secondary }))
      end

      let(:database) do
        client.database
      end

      let(:collection) do
        database[:with_read_pref]
      end

      it 'applies the options to the collection' do
        expect(collection.read_preference).to eq(Mongo::ServerSelector.get(mode: :secondary))
      end
    end
  end

  describe '#collection_names' do

    let(:database) do
      described_class.new(authorized_client, TEST_DB)
    end

    before do
      database[:users].create
    end

    after do
      database[:users].drop
    end

    it 'returns the stripped names of the collections' do
      expect(database.collection_names).to include('users')
    end

    it 'does not include system collections' do
      expect(database.collection_names).to_not include('system.indexes')
    end

    context 'when specifying a batch size' do

      it 'returns the stripped names of the collections' do
        expect(database.collection_names(batch_size: 1).to_a).to include('users')
      end
    end

    context 'when there are more collections than the initial batch size' do

      before do
        2.times do |i|
          database["#{i}_dalmatians"].create
        end
      end

      after do
        2.times do |i|
          database["#{i}_dalmatians"].drop
        end
      end

      it 'returns all collections' do
        expect(database.collection_names(batch_size: 1).select { |c| c =~ /dalmatians/}.size).to eq(2)
      end

    end
  end

  describe '#list_collections' do

    let(:database) do
      described_class.new(authorized_client, TEST_DB)
    end

    let(:result) do
      database.list_collections.map do |info|
        info['name']
      end
    end

    before do
      database[:users].create
    end

    after do
      database[:users].drop
    end

    it 'returns a list of the collections info', if: list_command_enabled?  do
      expect(result).to include('users')
    end

    it 'returns a list of the collections info', unless: list_command_enabled?  do
      expect(result).to include("#{TEST_DB}.users")
    end
  end

  describe '#collections' do

    context 'when the database exists' do

      let(:database) do
        described_class.new(authorized_client, TEST_DB)
      end

      let(:collection) do
        Mongo::Collection.new(database, 'users')
      end

      before do
        database[:users].create
      end

      after do
        database[:users].drop
      end

      it 'returns collection objects for each name' do
        expect(database.collections).to include(collection)
      end
    end

    context 'when the database does not exist' do

      let(:database) do
        described_class.new(authorized_client, 'invalid_database')
      end

      it 'returns an empty list', if: write_command_enabled? do
        expect(database.collections).to be_empty
      end
    end

    context 'when the user is not authorized', if: auth_enabled? do

      let(:database) do
        described_class.new(unauthorized_client, TEST_DB)
      end

      it 'raises an exception' do
        expect {
          database.collections
        }.to raise_error(Mongo::Error::OperationFailure)
      end
    end
  end

  describe '#command' do

    let(:database) do
      described_class.new(authorized_client, TEST_DB)
    end

    it 'sends the query command to the cluster' do
      expect(database.command(:ismaster => 1).written_count).to eq(0)
    end

    context 'when a read concern is provided', if: find_command_enabled? do

      context 'when the read concern is valid' do

        it 'sends the read concern' do
          expect {
            database.command(:ismaster => 1, readConcern: { level: 'local' })
          }.to_not raise_error
        end
      end

      context 'when the read concern is not valid' do

        it 'raises an exception', if: (find_command_enabled? && !sharded?) do
          expect {
            database.command(:ismaster => 1, readConcern: { level: 'yay' })
          }.to raise_error(Mongo::Error::OperationFailure)
        end
      end
    end

    context 'when no read preference is provided', unless: sharded? do

      let!(:primary_server) do
        database.cluster.next_primary
      end

      before do
        expect(primary_server).to receive(:with_connection).at_least(:once).and_call_original
      end

      it 'uses read preference of primary' do
        expect(database.command(ping: 1)).to be_successful
      end
    end

    context 'when the client has a read preference set', unless: sharded? do

      let!(:primary_server) do
        database.cluster.next_primary
      end

      let(:read_preference) do
        { :mode => :secondary, :tag_sets => [{ 'non' => 'existent' }] }
      end

      let(:client) do
        authorized_client.with(read: read_preference)
      end

      let(:database) do
        described_class.new(client, TEST_DB, client.options)
      end

      before do
        expect(primary_server).to receive(:with_connection).at_least(:once).and_call_original
      end

      it 'does not use the client read preference 'do
        expect(database.command(ping: 1)).to be_successful
      end
    end

    context 'when there is a read preference argument provided', unless: sharded? do

      let(:read_preference) do
        { :mode => :secondary, :tag_sets => [{ 'non' => 'existent' }] }
      end

      let(:client) do
        authorized_client.with(server_selection_timeout: 0.2)
      end

      let(:database) do
        described_class.new(client, TEST_DB, client.options)
      end

      before do
        allow(database.cluster).to receive(:single?).and_return(false)
      end

      it 'uses the read preference argument' do
        expect {
          database.command({ ping: 1 }, read: read_preference)
        }.to raise_error(Mongo::Error::NoServerAvailable)
      end
    end

    context 'when the client has a server_selection_timeout set', unless: sharded? do

      let(:client) do
        authorized_client.with(server_selection_timeout: 0)
      end

      let(:database) do
        described_class.new(client, TEST_DB, client.options)
      end

      it 'uses the client server_selection_timeout' do
        expect {
          database.command(ping: 1)
        }.to raise_error(Mongo::Error::NoServerAvailable)
      end
    end

    context 'when a write concern is not defined on the client/database object' do

      context 'when a write concern is provided in the selector', if: standalone? do

        let(:cmd) do
          {
              insert: TEST_COLL,
              documents: [ { a: 1 } ],
              writeConcern: INVALID_WRITE_CONCERN
          }
        end

        it 'uses the write concern' do
          expect {
            database.command(cmd)
          }.to raise_exception(Mongo::Error::OperationFailure)
        end
      end
    end

    context 'when a write concern is defined on the client/database object' do

      let(:client_options) do
        {
          write: INVALID_WRITE_CONCERN
        }
      end

      let(:database) do
        described_class.new(authorized_client.with(client_options), TEST_DB)
      end

      context 'when a write concern is not in the command selector', if: write_command_enabled? do

        let(:cmd) do
          {
              insert: TEST_COLL,
              documents: [ { a: 1 } ]
          }
        end

        it 'does not apply a write concern' do
          expect(database.command(cmd).written_count).to eq(1)
        end
      end

      context 'when a write concern is provided in the command selector', if: write_command_enabled? && standalone? do

        let(:cmd) do
          {
              insert: TEST_COLL,
              documents: [ { a: 1 } ],
              writeConcern: INVALID_WRITE_CONCERN
          }
        end

        it 'uses the write concern' do
          expect {
            database.command(cmd)
          }.to raise_exception(Mongo::Error::OperationFailure)
        end
      end
    end
  end

  describe '#drop' do

    let(:database) do
      described_class.new(authorized_client, TEST_DB)
    end

    it 'drops the database', if: write_command_enabled? do
      expect(database.drop).to be_successful
    end

    it 'raises an exception', if: (!write_command_enabled? && auth_enabled?) do
      expect {
        database.drop
      }.to raise_error(Mongo::Error::OperationFailure)
    end

    context 'when the client/database has a write concern' do

      let(:client_options) do
        {
          write: INVALID_WRITE_CONCERN,
          database: :safe_to_drop
        }
      end

      let(:client) do
        root_authorized_client.with(client_options)
      end

      let(:database_with_write_options) do
        client.database
      end

      context 'when the server supports write concern on the dropDatabase command', if: (collation_enabled? && standalone?) do

        it 'applies the write concern' do
          expect{
            database_with_write_options.drop
          }.to raise_exception(Mongo::Error::OperationFailure)
        end
      end

      context 'when the server does not support write concern on the dropDatabase command', unless: collation_enabled? do

        it 'does not apply the write concern' do
          expect(database_with_write_options.drop).to be_successful
        end
      end
    end
  end

  describe '#initialize' do

    context 'when provided a valid name' do

      let(:database) do
        described_class.new(authorized_client, TEST_DB)
      end

      it 'sets the name as a string' do
        expect(database.name).to eq(TEST_DB)
      end

      it 'sets the client' do
        expect(database.client).to eq(authorized_client)
      end
    end

    context 'when the name is nil' do

      it 'raises an error' do
        expect do
          described_class.new(authorized_client, nil)
        end.to raise_error(Mongo::Error::InvalidDatabaseName)
      end
    end
  end

  describe '#inspect' do

    let(:database) do
      described_class.new(authorized_client, TEST_DB)
    end

    it 'includes the object id' do
      expect(database.inspect).to include(database.object_id.to_s)
    end

    it 'includes the name' do
      expect(database.inspect).to include(database.name)
    end
  end

  describe '#fs', unless: sharded? do

    let(:database) do
      described_class.new(authorized_client, TEST_DB)
    end

    shared_context 'a GridFS database' do

      it 'returns a Grid::FS for the db' do
        expect(fs).to be_a(Mongo::Grid::FSBucket)
      end

      context 'when operating on the fs' do

        let(:file) do
          Mongo::Grid::File.new('Hello!', :filename => 'test.txt')
        end

        after do
          fs.files_collection.delete_many
          fs.chunks_collection.delete_many
        end

        let(:from_db) do
          fs.insert_one(file)
          fs.find({ filename: 'test.txt' }, limit: 1).first
        end

        it 'returns the assembled file from the db' do
          expect(from_db['filename']).to eq(file.info.filename)
        end
      end
    end

    context  'when no options are provided' do

      let(:fs) do
        database.fs
      end

      it_behaves_like 'a GridFS database'
    end

    context 'when a custom prefix is provided' do

      context 'when the option is fs_name' do

        let(:fs) do
          database.fs(:fs_name => 'grid')
        end

        it 'sets the custom prefix' do
          expect(fs.prefix).to eq('grid')
        end

        it_behaves_like 'a GridFS database'
      end

      context 'when the option is bucket_name' do

        let(:fs) do
          database.fs(:bucket_name => 'grid')
        end

        it 'sets the custom prefix' do
          expect(fs.prefix).to eq('grid')
        end

        it_behaves_like 'a GridFS database'
      end
    end
  end
end
