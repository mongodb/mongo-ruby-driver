require 'spec_helper'

describe Mongo::Client do

  before do
    if running_ssl?
      allow_any_instance_of(Mongo::Server::Monitor).to receive(:ismaster) do
        [{}, 1]
      end
    end
  end

  describe '#==' do

    let(:client) do
      described_class.new(
        ['127.0.0.1:27017'],
        :read => { :mode => :primary },
        :database => TEST_DB
      )
    end

    after do
      client.close
    end

    context 'when the other is a client' do

      context 'when the options and cluster are equal' do

        let(:other) do
          described_class.new(
            ['127.0.0.1:27017'],
            :read => { :mode => :primary },
            :database => TEST_DB
          )
        end

        it 'returns true' do
          expect(client).to eq(other)
        end
      end

      context 'when the options and cluster are not equal' do

        let(:other) do
          described_class.new(
            ['127.0.0.1:27017'],
            :read => { :mode => :secondary },
            :database => TEST_DB
          )
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

  describe '#eql' do

    let(:client) do
      described_class.new(
        ['127.0.0.1:27017'],
        :read => { :mode => :primary },
        :database => TEST_DB
      )
    end

    context 'when the other is a client' do

      context 'when the options and cluster are equal' do

        let(:other) do
          described_class.new(
            ['127.0.0.1:27017'],
            :read => { :mode => :primary },
            :database => TEST_DB
          )
        end

        it 'returns true' do
          expect(client).to eql(other)
        end
      end

      context 'when the options and cluster are not equal' do

        let(:other) do
          described_class.new(
            ['127.0.0.1:27017'],
            :read => { :mode => :secondary },
            :database => TEST_DB
          )
        end

        it 'returns true' do
          expect(client).not_to eql(other)
        end
      end
    end

    context 'when the other is not a client' do

      let(:client) do
        described_class.new(
          ['127.0.0.1:27017'],
          :read => { :mode => :primary },
          :database => TEST_DB
        )
      end

      it 'returns false' do
        expect(client).not_to eql('test')
      end
    end
  end

  describe '#hash' do

    let(:client) do
      described_class.new(
        ['127.0.0.1:27017'],
        :read => { :mode => :primary },
        :local_threshold => 10,
        :server_selection_timeout => 10000,
        :database => TEST_DB
      )
    end

    let(:options) do
      Mongo::Options::Redacted.new(:read => { :mode => :primary },
                                    :local_threshold => 10,
                                    :server_selection_timeout => 10000,
                                    :database => TEST_DB)
    end

    let(:expected) do
      [client.cluster, options].hash
    end

    it 'returns a hash of the cluster and options' do
      expect(client.hash).to eq(expected)
    end
  end

  describe '#inspect' do

    let(:client) do
      described_class.new(
        ['127.0.0.1:27017'],
        :read => { :mode => :primary },
        :database => TEST_DB
      )
    end

    it 'returns the cluster information' do
      expect(client.inspect).to include(
        "<Mongo::Client:0x#{client.object_id} cluster=127.0.0.1:27017"
      )
    end

    context 'when there is sensitive data in the options' do

      let(:client) do
        described_class.new(
            ['127.0.0.1:27017'],
            :read => { :mode => :primary },
            :database => TEST_DB,
            :password => 'some_password',
            :user => 'emily'
        )
      end

      it 'does not print out sensitive data' do
        expect(client.inspect).not_to match('some_password')
      end
    end
  end

  describe '#initialize' do

    context 'when providing options' do

      context 'when no database is provided' do

        let(:client) do
          described_class.new(['127.0.0.1:27017'], :read => { :mode => :secondary })
        end

        it 'defaults the database to admin' do
          expect(client.database.name).to eq('admin')
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

      context 'when providing a custom logger' do

        let(:logger) do
          Logger.new($stdout).tap do |l|
            l.level = Logger::FATAL
          end
        end

        let(:client) do
          authorized_client.with(logger: logger)
        end

        after do
          client.close
        end

        it 'does not use the global logger' do
          expect(client.cluster.logger).to_not eq(Mongo::Logger.logger)
        end
      end
    end

    context 'when providing a connection string' do

      context 'when a database is provided' do

        let!(:uri) do
          'mongodb://127.0.0.1:27017/testdb'
        end

        let(:client) do
          described_class.new(uri)
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
          described_class.new(uri)
        end

        it 'defaults the database to admin' do
          expect(client.database.name).to eq('admin')
        end
      end

      context 'when options are provided' do

        let!(:uri) do
          'mongodb://127.0.0.1:27017/testdb?w=3'
        end

        let(:client) do
          described_class.new(uri)
        end

        let(:expected_options) do
          Mongo::Options::Redacted.new(:write => { :w => 3 }, :database => 'testdb')
        end

        it 'sets the options' do
          expect(client.options).to eq(expected_options)
        end
      end

      context 'when options are provided not in the string' do

        let!(:uri) do
          'mongodb://127.0.0.1:27017/testdb'
        end

        let(:client) do
          described_class.new(uri, :write => { :w => 3 })
        end

        let(:expected_options) do
          Mongo::Options::Redacted.new(:write => { :w => 3 }, :database => 'testdb')
        end

        it 'sets the options' do
          expect(client.options).to eq(expected_options)
        end
      end

      context 'when options are provided in the string and explicitly' do

        let!(:uri) do
          'mongodb://127.0.0.1:27017/testdb?w=3'
        end

        let(:client) do
          described_class.new(uri, :write => { :w => 4 })
        end

        let(:expected_options) do
          Mongo::Options::Redacted.new(:write => { :w => 4 }, :database => 'testdb')
        end

        it 'allows explicit options to take preference' do
          expect(client.options).to eq(expected_options)
        end
      end

      context 'when a replica set name is provided' do

        let!(:uri) do
          'mongodb://127.0.0.1:27017/testdb?replicaSet=testing'
        end

        let(:client) do
          described_class.new(uri)
        end

        it 'sets the correct cluster topology' do
          expect(client.cluster.topology).to be_a(Mongo::Cluster::Topology::ReplicaSet)
        end
      end

      context 'when an invalid option is provided' do

        let(:client) do
          described_class.new(['127.0.0.1:27017'], :ssl => false, :invalid => :test)
        end

        it 'does not set the option' do
          expect(client.options.keys).not_to include('invalid')
        end

        it 'sets the valid options' do
          expect(client.options.keys).to include('ssl')
        end

        it 'warns that an invalid option has been specified' do
          expect(Mongo::Logger.logger).to receive(:warn)
          expect(client.options.keys).not_to include('invalid')
        end
      end
    end
  end

  describe '#read_preference' do

    let(:client) do
      described_class.new(['127.0.0.1:27017'],
                          :database => TEST_DB,
                          :read => mode,
                          :server_selection_timeout => 2)
    end

    let(:preference) do
      client.read_preference
    end

    context 'when mode is primary' do

      let(:mode) do
        { :mode => :primary }
      end

      it 'returns a primary read preference' do
        expect(preference).to be_a(Mongo::ServerSelector::Primary)
      end

      it 'passes the options to the read preference' do
        expect(preference.options[:server_selection_timeout]).to eq(2)
      end
    end

    context 'when mode is primary_preferred' do

      let(:mode) do
        { :mode => :primary_preferred }
      end

      it 'returns a primary preferred read preference' do
        expect(preference).to be_a(Mongo::ServerSelector::PrimaryPreferred)
      end
    end

    context 'when mode is secondary' do

      let(:mode) do
        { :mode => :secondary }
      end

      it 'returns a secondary read preference' do
        expect(preference).to be_a(Mongo::ServerSelector::Secondary)
      end
    end

    context 'when mode is secondary preferred' do

      let(:mode) do
        { :mode => :secondary_preferred }
      end

      it 'returns a secondary preferred read preference' do
        expect(preference).to be_a(Mongo::ServerSelector::SecondaryPreferred)
      end
    end

    context 'when mode is nearest' do

      let(:mode) do
        { :mode => :nearest }
      end

      it 'returns a nearest read preference' do
        expect(preference).to be_a(Mongo::ServerSelector::Nearest)
      end
    end

    context 'when no mode provided' do

      let(:mode) do
        {}
      end

      it 'returns a primary read preference' do
        expect(preference).to be_a(Mongo::ServerSelector::Primary)
      end
    end

    context 'when the read preference is printed' do

      let(:client) do
        described_class.new([ DEFAULT_ADDRESS ], options)
      end

      let(:options) do
        { user: 'Emily', password: 'sensitive_data', server_selection_timeout: 0.1 }
      end

      before do
        allow(client.database.cluster).to receive(:single?).and_return(false)
      end

      let(:error) do
        begin
          client.database.command(ping: 1)
        rescue => e
          e
        end
      end

      it 'redacts sensitive client options' do
        expect(error.message).not_to match(options[:password])
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

      it 'keeps the same cluster' do
        expect(database.cluster).to equal(client.cluster)
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

      it 'raises an exception' do
        expect {
          client.use(nil)
        }.to raise_error(Mongo::Error::InvalidDatabaseName)
      end
    end
  end

  describe '#with' do

    let(:client) do
      described_class.new(['127.0.0.1:27017'], :database => TEST_DB)
    end

    context 'when providing nil' do

      it 'returns the cloned client' do
        expect(client.with(nil)).to eq(client)
      end
    end

    context 'when the write concern is not changed' do

      let(:client) do
        described_class.new(
          ['127.0.0.1:27017'],
          :read => { :mode => :secondary }, :write => { :w => 1 }, :database => TEST_DB
        )
      end

      let!(:new_client) do
        client.with(:read => { :mode => :primary })
      end

      let(:new_options) do
        Mongo::Options::Redacted.new(:read => { :mode => :primary },
                                             :write => { :w => 1 },
                                             :database => TEST_DB)
      end

      let(:original_options) do
        Mongo::Options::Redacted.new(:read => { :mode => :secondary },
                                             :write => { :w => 1 },
                                             :database => TEST_DB)
      end

      it 'returns a new client' do
        expect(new_client).not_to equal(client)
      end

      it 'replaces the existing options' do
        expect(new_client.options).to eq(new_options)
      end

      it 'does not modify the original client' do
        expect(client.options).to eq(original_options)
      end

      it 'keeps the same cluster' do
        expect(new_client.cluster).to equal(client.cluster)
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

    context 'when an invalid option is provided' do

      let(:new_client) do
        client.with(invalid: :option, ssl: false)
      end

      it 'does not set the invalid option' do
        expect(new_client.options.keys).not_to include('invalid')
      end

      it 'sets the valid options' do
        expect(new_client.options.keys).to include('ssl')
      end

      it 'warns that an invalid option has been specified' do
        expect(Mongo::Logger.logger).to receive(:warn)
        expect(new_client.options.keys).not_to include('invalid')
      end
    end
  end

  describe '#write_concern' do

    let(:concern) { client.write_concern }

    context 'when no option was provided to the client' do

      let(:client) { described_class.new(['127.0.0.1:27017'], :database => TEST_DB) }

      it 'does not set the write concern' do
        expect(concern).to be_nil
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

          it 'raises an error' do
            expect {
              concern
            }.to raise_error(Mongo::Error::InvalidWriteConcern)
          end
        end
      end
    end
  end

  describe '#database_names' do

    it 'returns a list of database names' do
      expect(root_authorized_client.database_names).to include(
        'admin'
      )
    end
  end

  describe '#list_databases' do

    it 'returns a list of database info documents' do
      expect(
        root_authorized_client.list_databases.collect do |i|
          i['name']
        end).to include('admin')
    end
  end

    describe '#close' do

    let(:client) do
      described_class.new(['127.0.0.1:27017'])
    end

    before do
      expect(client.cluster).to receive(:disconnect!).and_call_original
    end

    it 'disconnects the cluster and returns true' do
      expect(client.close).to be(true)
    end
  end

  describe '#reconnect' do

    let(:client) do
      described_class.new(['127.0.0.1:27017'])
    end

    before do
      expect(client.cluster).to receive(:reconnect!).and_call_original
    end

    it 'reconnects the cluster and returns true' do
      expect(client.reconnect).to be(true)
    end
  end

  describe '#dup' do

    let(:client) do
      described_class.new(
          ['127.0.0.1:27017'],
          :read => { :mode => :primary },
          :database => TEST_DB
      )
    end

    it 'creates a client with Redacted options' do
      expect(client.dup.options).to be_a(Mongo::Options::Redacted)
    end
  end

  describe '#collections' do

    before do
      authorized_client.database[:users].create
    end

    after do
      authorized_client.database[:users].drop
    end

    let(:collection) do
      Mongo::Collection.new(authorized_client.database, 'users')
    end

    it 'refers the current database collections' do
      expect(authorized_client.collections).to include(collection)
      expect(authorized_client.collections).to all(be_a(Mongo::Collection))
    end
  end
end
