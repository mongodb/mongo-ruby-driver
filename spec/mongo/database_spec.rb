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

    let(:client) do
      Mongo::Client.new([ '127.0.0.1:27017' ], database: :test)
    end

    let(:database) do
      described_class.new(client, :test)
    end

    before do
      # @todo: Add condition variable.
      client.cluster.scan!
    end

    it 'sends the query command to the cluster' do
      expect(database.command(:ismaster => 1)['ok']).to eq(1)
    end
  end

  describe '#drop_collection' do

    let(:client)   { Mongo::Client.new([ '127.0.0.1:27017' ], database: :test) }
    let(:database) { described_class.new(client, :test) }
    let(:coll)     { 'somecollection' }

    before do
      client.cluster.scan!
      database[coll].insert({ :name => 'Sally' })
      database.drop_collection(coll)
    end

    it 'drops the collection' do
      expect(database.collection_names.include?(coll)).to eq(false)
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

  describe '#rename_collection' do

    let(:client)   { Mongo::Client.new([ '127.0.0.1:27017' ], database: :test) }
    let(:database) { described_class.new(client, :test) }

    context 'when new name is invalid' do

      it 'raises an error' do
        expect{database.rename_collection('oldname', '$$$..$$$')}.to raise_error
      end
    end

    context 'when new name is valid' do

      let(:old) { 'oldname' }
      let(:new) { 'newname' }

      before do
        client.cluster.scan!
        database.drop_collection(old)
        database.drop_collection(new)
      end

      context 'when drop is false' do

        context 'when collection with new name already exists' do

          before do
            database[old].insert({ :a => 1 })
            database[new].insert({ :b => 1 })
          end

          it 'raises an error' do
            expect{database.rename_collection(old, new, false)}.to raise_error
          end
        end

        context 'no collection with new name exists' do

          before do
            database[old].insert({ :a => 1 })
            database.rename_collection(old, new, false)
          end

          it 'renames the collection' do
            expect(database[new].find_one['a']).to eq(1)
          end
        end
      end

      context 'when drop is true' do

        context 'when collection with same name already exists' do

          before do
            database[old].insert({ :a => 1 })
            database[new].insert({ :b => 1 })
            database.rename_collection(old, new, true)
          end

          it 'replaces that collection with this one' do
            expect(database[new].find_one['a']).to eq(1)
          end
        end
      end
    end
  end

  describe '#server_preference' do

    context 'the passed-in options have a server preference' do

      let(:database)    { described_class.new(client, :test) }
      let(:read)        { :secondary_preferred }

      it 'returns the operation-level server preference' do
        expect(database.server_preference({ :read => read }).name).to be(read)
      end
    end

    context 'the passed-in options have no server preference' do

      context 'the database has a global server preference' do

        let(:db_read)  { :nearest }
        let(:database) { described_class.new(client, :test, { :read => db_read }) }

        it 'returns db-level server preference' do
          expect(database.server_preference.name).to eq(db_read)
        end
      end

      context 'there is no global db-level server preference' do

        let(:database)    { described_class.new(client, :test) }
        let(:client_read) { :secondary_preferred }
        let(:client_pref) { Mongo::ServerPreference.get(:mode => client_read) }

        it 'returns the client-level server preference' do
          client.stub(:server_preference).and_return(client_pref)
          expect(database.server_preference.name).to eq(client_read)
          expect(client).to have_received(:server_preference)
        end
      end
    end
  end
end
