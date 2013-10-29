require 'spec_helper'

describe Mongo::Client do

  describe '#==' do

    let(:client) do
      described_class.new(['127.0.0.1:27017'], :read => :primary)
    end

    context 'when the other is a client' do

      context 'when the options and cluster are equal' do

        let(:other) do
          described_class.new(['127.0.0.1:27017'], :read => :primary)
        end

        it 'returns true' do
          expect(client).to eq(other)
        end
      end

      context 'when the options and cluster are not equal' do

        let(:other) do
          described_class.new(['127.0.0.1:27017'], :read => :secondary)
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
      described_class.new(['127.0.0.1:27017'])
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

    context 'when a database has not been selected' do

      it 'raises an error' do
        expect do
          client[:users]
        end.to raise_error(Mongo::Client::NoDatabase)
      end
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
      it 'does not set the database' do
        expect { client[:users] }.to raise_error(Mongo::Client::NoDatabase)
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
        expect(client.options).to eq(:write => { :w => 3 })
      end
    end
  end

  describe '#eql' do

    let(:client) do
      described_class.new(['127.0.0.1:27017'], :read => :primary)
    end

    context 'when the other is a client' do

      context 'when the options and cluster are equal' do

        let(:other) do
          described_class.new(['127.0.0.1:27017'], :read => :primary)
        end

        it 'returns true' do
          expect(client).to eql(other)
        end
      end

      context 'when the options and cluster are not equal' do

        let(:other) do
          described_class.new(['127.0.0.1:27017'], :read => :secondary)
        end

        it 'returns true' do
          expect(client).not_to eql(other)
        end
      end
    end

    context 'when the other is not a client' do

      let(:client) do
        described_class.new(['127.0.0.1:27017'], :read => :primary)
      end

      it 'returns false' do
        expect(client).not_to eql('test')
      end
    end
  end

  describe '#hash' do

    let(:client) do
      described_class.new(['127.0.0.1:27017'], :read => :primary)
    end

    let(:expected) do
      [client.cluster, { :read => :primary }].hash
    end

    it 'returns a hash of the cluster and options' do
      expect(client.hash).to eq(expected)
    end
  end

  describe '#inspect' do

    let(:client) do
      described_class.new(
        ['1.0.0.1:2', '1.0.0.1:1'],
        :read => :primary
      )
    end

    it 'returns the cluster information' do
      expect(client.inspect).to eq(
        "<Mongo::Client:0x#{client.object_id} cluster=1.0.0.1:2, 1.0.0.1:1>"
      )
    end
  end

  describe '#initialize' do

    context 'when providing no options' do

      let(:client) do
        described_class.new(['127.0.0.1:27017'])
      end

      it 'sets the options to empty' do
        expect(client.options).to be_empty
      end

      it 'sets the cluster' do
        expect(client.cluster).to be_a(Mongo::Cluster)
      end
    end

    context 'when providing options' do

      context 'when no database is provided' do

        let(:client) do
          described_class.new(['127.0.0.1:27017'], :read => :secondary)
        end

        it 'sets the options on the client' do
          expect(client.options).to eq(:read => :secondary)
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

  describe '#use' do

    let(:client) do
      described_class.new(['127.0.0.1:27017'])
    end

    shared_examples_for 'a database switching object' do

      it 'returns the new database' do
        expect(database.name).to eq('testdb')
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
          :read => :secondary, :write => { :w => 1 }
        )
      end

      let!(:new_client) do
        client.with(:read => :primary)
      end

      it 'returns a new client' do
        expect(new_client).not_to equal(client)
      end

      it 'replaces the existing options' do
        expect(new_client.options).to eq(:read => :primary,
                                         :write => { :w => 1 })
      end

      it 'does not modify the original client' do
        expect(client.options).to eq(:read => :secondary,
                                     :write => { :w => 1 })
      end

      it 'clones the cluster addresses' do
        expect do
          new_client.cluster.addresses
        end.not_to equal(client.cluster.addresses)
      end
    end

    context 'when the write concern is changed' do

      let(:client) do
        described_class.new(['127.0.0.1:27017'], :write => { :w => 1 })
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

      let(:client) { described_class.new(['127.0.0.1:27017']) }

      it 'returns a acknowledged write concern' do
        expect(concern.get_last_error).to eq(:getlasterror => 1, :w => 1)
      end
    end

    context 'when an option is provided' do

      context 'when the option is acknowledged' do

        let(:client) do
          described_class.new(['127.0.0.1:27017'], :write => { :j => true })
        end

        it 'returns a acknowledged write concern' do
          expect(concern.get_last_error).to eq(:getlasterror => 1, :j => true)
        end
      end

      context 'when the option is unacknowledged' do

        context 'when the w is 0' do

          let(:client) do
            described_class.new(['127.0.0.1:27017'], :write => { :w => 0 })
          end

          it 'returns an unacknowledged write concern' do
            expect(concern.get_last_error).to be_nil
          end
        end

        context 'when the w is -1' do

          let(:client) do
            described_class.new(['127.0.0.1:27017'], :write => { :w => -1 })
          end

          it 'returns an unacknowledged write concern' do
            expect(concern.get_last_error).to be_nil
          end
        end
      end
    end
  end
end
