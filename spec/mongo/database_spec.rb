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
      expect(database.command(:ismaster => 1).n).to be_nil
    end
  end

  describe '#drop' do

    let(:database) do
      described_class.new(authorized_client, TEST_DB)
    end

    it 'drops the database', if: write_command_enabled? do
      expect(database.drop).to be_ok
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
end
