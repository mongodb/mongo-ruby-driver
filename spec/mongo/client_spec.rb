require 'spec_helper'

describe Mongo::Client do

  describe '#==' do

    let(:client) do
      described_class.new(['127.0.0.1:27017'], :read => :primary, :database => TEST_DB)
    end

    context 'when the other is a client' do

      context 'when the options and cluster are equal' do

        let(:other) do
          described_class.new(['127.0.0.1:27017'], :read => :primary, :database => TEST_DB)
        end

        it 'returns true' do
          expect(client).to eq(other)
        end
      end

      context 'when the options and cluster are not equal' do

        let(:other) do
          described_class.new(['127.0.0.1:27017'], :read => :secondary, :database => TEST_DB)
        end

        it 'returns true' do
          expect(client).not_to eq(other)
        end
      end
    end

    context 'when the other is not a client' do

      it 'returns false' do
        expect(client).not_to eq('test')
      end
    end
  end

  describe '#[]' do

    let(:client) do
      described_class.new(['127.0.0.1:27017'], :database => TEST_DB)
    end

    shared_examples_for 'a collection switching object' do

      before do
        client.use(:dbtest)
      end

      it 'returns the new collection' do
        expect(collection.name).to eq('users')
      end
    end

    context 'when provided a string' do

      let(:collection) do
        client['users']
      end

      it_behaves_like 'a collection switching object'
    end

    context 'when provided a symbol' do

      let(:collection) do
        client[:users]
      end

      it_behaves_like 'a collection switching object'
    end
  end

  describe '.connect' do

    context 'when a database is provided' do

      let!(:uri) do
        'mongodb://127.0.0.1:27017/testdb'
      end

      let(:client) do
        described_class.connect(uri)
      end

      it 'sets the database' do
        expect { client[:users] }.to_not raise_error
      end
    end

    context 'when a database is not provided' do

      let!(:uri) do
        'mongodb://127.0.0.1:27017'
      end

      let(:client) do
        described_class.connect(uri)
      end

      it 'raises an error' do
        expect { client }.to raise_error(Mongo::Database::InvalidName)
      end
    end

    context 'when options are provided' do

      let!(:uri) do
        'mongodb://127.0.0.1:27017/testdb?w=3'
      end

      let(:client) do
        described_class.connect(uri)
      end

      it 'sets the options' do
        expect(client.options).to eq(:write => { :w => 3 }, :database => 'testdb')
      end
    end
  end

  describe '#eql' do

    let(:client) do
      described_class.new(['127.0.0.1:27017'], :read => :primary, :database => TEST_DB)
    end

    context 'when the other is a client' do

      context 'when the options and cluster are equal' do

        let(:other) do
          described_class.new(['127.0.0.1:27017'], :read => :primary, :database => TEST_DB)
        end

        it 'returns true' do
          expect(client).to eql(other)
        end
      end

      context 'when the options and cluster are not equal' do

        let(:other) do
          described_class.new(['127.0.0.1:27017'], :read => :secondary, :database => TEST_DB)
        end

        it 'returns true' do
          expect(client).not_to eql(other)
        end
      end
    end

    context 'when the other is not a client' do

      let(:client) do
        described_class.new(['127.0.0.1:27017'], :read => :primary, :database => TEST_DB)
      end

      it 'returns false' do
        expect(client).not_to eql('test')
      end
    end
  end

  describe '#hash' do

    let(:client) do
      described_class.new(['127.0.0.1:27017'], :read => :primary, :database => TEST_DB)
    end

    let(:expected) do
      [client.cluster, { :read => :primary, :database => TEST_DB }].hash
    end

    it 'returns a hash of the cluster and options' do
      expect(client.hash).to eq(expected)
    end
  end

  describe '#inspect' do

    let(:client) do
      described_class.new(['127.0.0.1:27017'], :read => :primary, :database => TEST_DB)
    end

    it 'returns the cluster information' do
      expect(client.inspect).to eq(
        "<Mongo::Client:0x#{client.object_id} cluster=127.0.0.1:27017>"
      )
    end
  end

  describe '#initialize' do

    context 'when providing options' do

      context 'when no database is provided' do

        let(:client) do
          described_class.new(['127.0.0.1:27017'], :read => :secondary, :database => TEST_DB)
        end

        it 'sets the options on the client' do
          expect(client.options).to eq(:read => :secondary, :database => TEST_DB)
        end
      end

      context 'when a database is provided' do

        let(:client) do
          described_class.new(['127.0.0.1:27017'], :database => :testdb)
        end

        it 'sets the current database' do
          expect(client[:users].name).to eq('users')
        end
      end
    end
  end

  describe '#server_preference' do

    let(:client) do
      described_class.new(['127.0.0.1:27017'], :database => TEST_DB, :read => mode)
    end

    let(:preference) do
      client.server_preference
    end

    context 'when mode is primary' do

      let(:mode) do
        { :mode => :primary }
      end

      it 'returns a primary server preference' do
        expect(preference).to be_a(Mongo::ServerPreference::Primary)
      end
    end

    context 'when mode is primary_preferred' do

      let(:mode) do
        { :mode => :primary_preferred }
      end

      it 'returns a primary preferred server preference' do
        expect(preference).to be_a(Mongo::ServerPreference::PrimaryPreferred)
      end
    end

    context 'when mode is secondary' do

      let(:mode) do
        { :mode => :secondary }
      end

      it 'returns a secondary server preference' do
        expect(preference).to be_a(Mongo::ServerPreference::Secondary)
      end
    end

    context 'when mode is secondary preferred' do

      let(:mode) do
        { :mode => :secondary_preferred }
      end

      it 'returns a secondary preferred server preference' do
        expect(preference).to be_a(Mongo::ServerPreference::SecondaryPreferred)
      end
    end

    context 'when mode is nearest' do

      let(:mode) do
        { :mode => :nearest }
      end

      it 'returns a nearest server preference' do
        expect(preference).to be_a(Mongo::ServerPreference::Nearest)
      end
    end

    context 'when no mode provided' do

      let(:mode) do
        {}
      end

      it 'returns a primary server preference' do
        expect(preference).to be_a(Mongo::ServerPreference::Primary)
      end
    end
  end

  describe '#use' do

    let(:client) do
      described_class.new(['127.0.0.1:27017'], :database => TEST_DB)
    end

    shared_examples_for 'a database switching object' do

      it 'returns the new client' do
        expect(client.send(:database).name).to eq('ruby-driver')
      end
    end

    context 'when provided a string' do

      let(:database) do
        client.use('testdb')
      end

      it_behaves_like 'a database switching object'
    end

    context 'when provided a symbol' do

      let(:database) do
        client.use(:testdb)
      end

      it_behaves_like 'a database switching object'
    end

    context 'when providing nil' do

      it 'raises an error' do
        expect do
          client.use(nil)
        end.to raise_error(Mongo::Database::InvalidName)
      end
    end
  end

  describe '#with' do

    context 'when the write concern is not changed' do

      let(:client) do
        described_class.new(
          ['127.0.0.1:27017'],
          :read => :secondary, :write => { :w => 1 }, :database => TEST_DB
        )
      end

      let!(:new_client) do
        client.with(:read => :primary)
      end

      it 'returns a new client' do
        expect(new_client).not_to equal(client)
      end

      it 'replaces the existing options' do
        expect(new_client.options).to eq(:read => :primary, :write => { :w => 1 }, :database => TEST_DB)
      end

      it 'does not modify the original client' do
        expect(client.options).to eq(:read => :secondary, :write => { :w => 1 }, :database => TEST_DB)
      end

      it 'clones the cluster addresses' do
        expect(new_client.cluster.addresses).not_to equal(client.cluster.addresses)
      end
    end

    context 'when the write concern is changed' do

      let(:client) do
        described_class.new(['127.0.0.1:27017'], :write => { :w => 1 }, :database => TEST_DB)
      end

      context 'when the write concern has not been accessed' do

        let!(:new_client) do
          client.with(:write => { :w => 0 })
        end

        let(:get_last_error) do
          new_client.write_concern.get_last_error
        end

        it 'returns the correct write concern' do
          expect(get_last_error).to be_nil
        end
      end

      context 'when the write concern has been accessed' do

        let!(:new_client) do
          client.write_concern
          client.with(:write => { :w => 0 })
        end

        let(:get_last_error) do
          new_client.write_concern.get_last_error
        end

        it 'returns the correct write concern' do
          expect(get_last_error).to be_nil
        end
      end
    end
  end

  describe '#write_concern' do

    let(:concern) { client.write_concern }

    context 'when no option was provided to the client' do

      let(:client) { described_class.new(['127.0.0.1:27017'], :database => TEST_DB) }

      it 'returns a acknowledged write concern' do
        expect(concern.get_last_error).to eq(:getlasterror => 1, :w => 1)
      end
    end

    context 'when an option is provided' do

      context 'when the option is acknowledged' do

        let(:client) do
          described_class.new(['127.0.0.1:27017'], :write => { :j => true }, :database => TEST_DB)
        end

        it 'returns a acknowledged write concern' do
          expect(concern.get_last_error).to eq(:getlasterror => 1, :j => true)
        end
      end

      context 'when the option is unacknowledged' do

        context 'when the w is 0' do

          let(:client) do
            described_class.new(['127.0.0.1:27017'], :write => { :w => 0 }, :database => TEST_DB)
          end

          it 'returns an unacknowledged write concern' do
            expect(concern.get_last_error).to be_nil
          end
        end

        context 'when the w is -1' do

          let(:client) do
            described_class.new(['127.0.0.1:27017'], :write => { :w => -1 }, :database => TEST_DB)
          end

          it 'returns an unacknowledged write concern' do
            expect(concern.get_last_error).to be_nil
          end
        end
      end
    end
  end

  describe '#index_cache' do

    let(:client) { described_class.new(['127.0.0.1:27017'], :database => TEST_DB) }
    let(:index)  { 'name_1' }
    let(:time)   { Time.now.utc.to_i }
    let(:ns)     { 'test.collection' }

    context 'we pass a String as a parameter' do

      context 'index does not exist in the cache' do

        it 'returns nil' do
          expect(client.index_cache(index, ns)).to be(nil)
        end
      end

      context 'index exists in the cache' do

        before do
          client.index_cache({ index => time }, ns)
        end

        it 'returns an Integer' do
          expect(client.index_cache(index, ns)).to be_a(Integer)
        end

        it 'returns its expiration time' do
          expect(client.index_cache(index, ns)).to eq(time)
        end
      end

      context 'same index name exists under a different ns' do

        before do
          client.index_cache({ index => time }, 'something.else')
        end

        it 'returns nil' do
          expect(client.index_cache(index, ns)).to be(nil)
        end
      end

      context 'there exists a different index under the same ns' do

        before do
          client.index_cache({ "something_else_1" => time }, ns)
        end

        it 'returns nil' do
          expect(client.index_cache(index, ns)).to be(nil)
        end
      end
    end

    context 'we pass a Hash as a parameter' do

      context 'index does not exist in the cache' do

        before do
          client.index_cache({ index => time }, ns)
        end

        it 'adds the index to the cache' do
          expect(client.index_cache(index, ns)).to eq(time)
        end
      end

      context 'index exists in the cache' do

        before do
          client.index_cache({ index => time }, ns)
          client.index_cache({ index => (time + 300) }, ns)
        end

        it 'writes over the previous entry for that index' do
          expect(client.index_cache(index, ns)).to eq(time + 300)
        end
      end
    end
  end
end
