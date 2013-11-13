require 'spec_helper'

describe Mongo::Database do

  let(:client) { double('client') }

  describe '#==' do

    let(:database) { described_class.new(client, :test) }

    context 'when the names are the same' do

      let(:other) { described_class.new(client, :test) }

      it 'returns true' do
        expect(database).to eq(other)
      end
    end

    context 'when the names are not the same' do

      let(:other) { described_class.new(client, :testing) }

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
      described_class.new(client, :test)
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

    let(:names) do
      [{ 'name' => 'test.users' }, { 'name' => 'test.sounds' }]
    end

    let(:collection) { double('collection') }
    let(:database) { described_class.new(client, :test) }

    before do
      expect(database).to receive(:collection).with(
        'system.namespaces').and_return(collection)

      expect(collection).to receive(:find).with(
        :name => { '$not' => /test\.system\,|\$/ }).and_return(names)
    end

    it 'returns the stripped names of the collections' do
      expect(database.collection_names).to eq(%w[users sounds])
    end
  end

  describe '#collections' do

    let(:database) { described_class.new(client, :test) }
    let(:collection) { Mongo::Collection.new(database, 'users') }

    before do
      expect(database).to receive(:collection_names).and_return(['users'])
    end

    it 'returns collection objects for each name' do
      expect(database.collections).to eq([collection])
    end
  end

  describe '#command' do

    let(:query) do
      Mongo::Protocol::Query.new(
        'test',
        '$cmd',
        { :ismaster => 1 },
        :limit => -1,
        :read => :secondary
      )
    end

    let(:cluster) { double('cluster') }
    let(:database) { described_class.new(client, :test) }

    before do
      expect(client).to receive(:cluster).and_return(cluster)
      expect(client).to receive(:read_preference).and_return(:secondary)
      expect(cluster).to receive(:execute).with(query).and_return(:ok => 1)
    end

    it 'sends the query command to the cluster' do
      expect(database.command(:ismaster => 1)).to eq(:ok => 1)
    end
  end

  describe '#initialize' do

    context 'when provided a valid name' do

      let(:database) do
        described_class.new(client, :test)
      end

      it 'sets the name as a string' do
        expect(database.name).to eq('test')
      end

      it 'sets the client' do
        expect(database.client).to eq(client)
      end
    end

    context 'when the name is nil' do

      it 'raises an error' do
        expect do
          described_class.new(client, nil)
        end.to raise_error(Mongo::Database::InvalidName)
      end
    end
  end
end
