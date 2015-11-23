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

    context 'when an alternate read preference is specified' do

      before do
        allow(database.cluster).to receive(:single?).and_return(false)
      end

      let(:read) do
        { :mode => :secondary, :tag_sets => [{ 'non' => 'existent' }] }
      end

      let(:client) do
        authorized_client.with(server_selection_timeout: 0.1)
      end

      let(:database) do
        described_class.new(client, TEST_DB, client.options)
      end

      it 'uses that read preference', unless: sharded? do
        expect do
          database.command({ ping: 1 }, { read: read })
        end.to raise_error(Mongo::Error::NoServerAvailable)
      end
    end

    context 'when there is a read preference set on the client' do

      let(:database) do
        described_class.new(authorized_client.with(read: { mode: :secondary }), TEST_DB)
      end

      it 'does not use the read preference' do
        expect(database.client.cluster).to receive(:next_primary).and_call_original
        database.command(ping: 1)
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

    it 'raises an exception', unless: write_command_enabled? do
      expect {
        database.drop
      }.to raise_error(Mongo::Error::OperationFailure)
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

  describe '#fs' do

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

        before do
          fs.insert_one(file)
        end

        after do
          fs.files_collection.delete_many
          fs.chunks_collection.delete_many
        end

        let(:from_db) do
          fs.find_one(:filename => 'test.txt')
        end

        it 'returns the assembled file from the db' do
          expect(from_db.filename).to eq(file.info.filename)
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
