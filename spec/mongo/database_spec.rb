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
        end.to raise_error(Mongo::Collection::InvalidName)
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
  end

  describe '#collections' do

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

  describe '#command' do

    let(:client) do
      Mongo::Client.new([ '127.0.0.1:27017' ], database: TEST_DB)
    end

    let(:database) do
      described_class.new(authorized_client, TEST_DB)
    end

    it 'sends the query command to the cluster' do
      expect(database.command(:ismaster => 1).written_count).to eq(0)
    end

    context 'when an alternate read preference is specified' do

      let(:read) do
        { :mode => :secondary,
          :tag_sets => [{ 'non' => 'existent' }] }
      end

      let(:client) do
        Mongo::Client.new([ '127.0.0.1:27017' ], database: TEST_DB)
      end

      let(:database) do
        described_class.new(authorized_client, TEST_DB)
      end

      it 'uses that read preference' do
        expect do
          database.command({ ping: 1 }, { read: read })
        end.to raise_error(Mongo::ServerPreference::NoServerAvailable)
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
      }.to raise_error(Mongo::Operation::Write::Failure)
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
        end.to raise_error(Mongo::Database::InvalidName)
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
        expect(fs).to be_a(Mongo::Grid::FS)
      end

      context 'when operating on the fs' do

        let(:file) do
          Mongo::Grid::File.new('Hello!', :filename => 'test.txt')
        end

        before do
          fs.insert_one(file)
        end

        after do
          fs.files_collection.find.remove_many
          fs.chunks_collection.find.remove_many
        end

        let(:from_db) do
          fs.find_one(:filename => 'test.txt')
        end

        it 'returns the assembled file from the db' do
          expect(from_db.filename).to eq(file.metadata.filename)
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

      let(:fs) do
        database.fs(:fs_name => 'grid')
      end

      it 'sets the custom prefix' do
        expect(fs.prefix).to eq('grid')
      end

      it_behaves_like 'a GridFS database'
    end
  end
end
