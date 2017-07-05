require 'spec_helper'

describe Mongo::Client do

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
        :local_threshold => 0.010,
        :server_selection_timeout => 10000,
        :database => TEST_DB
      )
    end

    let(:options) do
      Mongo::Options::Redacted.new(:read => { :mode => :primary },
                                    :local_threshold => 0.010,
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

      context 'when ssl options are provided' do

        let(:options) do
          {
              :ssl => true,
              :ssl_ca_cert => CA_PEM,
              :ssl_ca_cert_string => 'ca cert string',
              :ssl_ca_cert_object => 'ca cert object',
              :ssl_cert => CLIENT_CERT_PEM,
              :ssl_cert_string => 'cert string',
              :ssl_cert_object => 'cert object',
              :ssl_key => CLIENT_KEY_PEM,
              :ssl_key_string => 'key string',
              :ssl_key_object => 'key object',
              :ssl_key_pass_phrase => 'passphrase',
              :ssl_verify => true
          }
        end

        let(:client) do
          described_class.new(['127.0.0.1:27017'], TEST_OPTIONS.merge(options))
        end

        it 'sets the ssl option' do
          expect(client.options[:ssl]).to eq(options[:ssl])
        end

        it 'sets the ssl_ca_cert option' do
          expect(client.options[:ssl_ca_cert]).to eq(options[:ssl_ca_cert])
        end

        it 'sets the ssl_ca_cert_string option' do
          expect(client.options[:ssl_ca_cert_string]).to eq(options[:ssl_ca_cert_string])
        end

        it 'sets the ssl_ca_cert_object option' do
          expect(client.options[:ssl_ca_cert_object]).to eq(options[:ssl_ca_cert_object])
        end

        it 'sets the ssl_cert option' do
          expect(client.options[:ssl_cert]).to eq(options[:ssl_cert])
        end

        it 'sets the ssl_cert_string option' do
          expect(client.options[:ssl_cert_string]).to eq(options[:ssl_cert_string])
        end

        it 'sets the ssl_cert_object option' do
          expect(client.options[:ssl_cert_object]).to eq(options[:ssl_cert_object])
        end

        it 'sets the ssl_key option' do
          expect(client.options[:ssl_key]).to eq(options[:ssl_key])
        end

        it 'sets the ssl_key_string option' do
          expect(client.options[:ssl_key_string]).to eq(options[:ssl_key_string])
        end

        it 'sets the ssl_key_object option' do
          expect(client.options[:ssl_key_object]).to eq(options[:ssl_key_object])
        end

        it 'sets the ssl_key_pass_phrase option' do
          expect(client.options[:ssl_key_pass_phrase]).to eq(options[:ssl_key_pass_phrase])
        end

        it 'sets the ssl_verify option' do
          expect(client.options[:ssl_verify]).to eq(options[:ssl_verify])
        end
      end

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

      context 'when providing a heartbeat_frequency' do

        let(:client) do
          described_class.new(['127.0.0.1:27017'], :heartbeat_frequency => 2)
        end

        it 'sets the heartbeat frequency' do
          expect(client.cluster.options[:heartbeat_frequency]).to eq(client.options[:heartbeat_frequency])
        end
      end

      context 'when min_pool_size is provided' do

        let(:client) do
          described_class.new(['127.0.0.1:27017'], options)
        end

        context 'when max_pool_size is provided' do

          context 'when the min_pool_size is greater than the max_pool_size' do

            let(:options) do
              {
                  :min_pool_size => 20,
                  :max_pool_size => 10
              }
            end

            it 'raises an Exception' do
              expect {
                client
              }.to raise_exception(Mongo::Error::InvalidMinPoolSize)
            end
          end

          context 'when the min_pool_size is less than the max_pool_size' do

            let(:options) do
              {
                  :min_pool_size => 10,
                  :max_pool_size => 20
              }
            end

            it 'sets the option' do
              expect(client.options[:min_pool_size]).to eq(options[:min_pool_size])
              expect(client.options[:max_pool_size]).to eq(options[:max_pool_size])
            end
          end

          context 'when the min_pool_size is equal to the max_pool_size' do

            let(:options) do
              {
                  :min_pool_size => 10,
                  :max_pool_size => 10
              }
            end

            it 'sets the option' do
              expect(client.options[:min_pool_size]).to eq(options[:min_pool_size])
              expect(client.options[:max_pool_size]).to eq(options[:max_pool_size])
            end
          end
        end

        context 'when max_pool_size is not provided' do

          context 'when the min_pool_size is greater than the default max_pool_size' do

            let(:options) do
              {
                  :min_pool_size => 10
              }
            end

            it 'raises an Exception' do
              expect {
                client
              }.to raise_exception(Mongo::Error::InvalidMinPoolSize)
            end
          end

          context 'when the min_pool_size is less than the default max_pool_size' do

            let(:options) do
              {
                  :min_pool_size => 3
              }
            end

            it 'sets the option' do
              expect(client.options[:min_pool_size]).to eq(options[:min_pool_size])
            end
          end

          context 'when the min_pool_size is equal to the max_pool_size' do

            let(:options) do
              {
                :min_pool_size => Mongo::Server::ConnectionPool::Queue::MAX_SIZE
              }
            end

            it 'sets the option' do
              expect(client.options[:min_pool_size]).to eq(options[:min_pool_size])
            end
          end
        end
      end

      context 'when platform details are specified' do

        let(:app_metadata) do
          client.cluster.app_metadata
        end

        let(:client) do
          described_class.new(['127.0.0.1:27017'], :platform => 'mongoid-6.0.2')
        end

        it 'includes the platform info in the app metadata' do
          expect(app_metadata.send(:full_client_document)[:platform]).to match(/mongoid-6\.0\.2/)
        end
      end

      context 'when platform details are not specified' do

        let(:app_metadata) do
          client.cluster.app_metadata
        end

        let(:client) do
          described_class.new(['127.0.0.1:27017'])
        end

        let(:platform_string) do
          [
            RUBY_VERSION,
            RUBY_PLATFORM,
            RbConfig::CONFIG['build']
          ].join(', ')
        end

        it 'does not include the platform info in the app metadata' do
          expect(app_metadata.send(:full_client_document)[:platform]).to eq(platform_string)
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

        context 'when min_pool_size is provided' do

          context 'when max_pool_size is provided' do

            context 'when the min_pool_size is greater than the max_pool_size' do

              let(:uri) do
                'mongodb://127.0.0.1:27017/?minPoolSize=20&maxPoolSize=10'
              end

              it 'raises an Exception' do
                expect {
                  client
                }.to raise_exception(Mongo::Error::InvalidMinPoolSize)
              end
            end

            context 'when the min_pool_size is less than the max_pool_size' do

              let(:uri) do
                'mongodb://127.0.0.1:27017/?minPoolSize=10&maxPoolSize=20'
              end

              it 'sets the option' do
                expect(client.options[:min_pool_size]).to eq(10)
                expect(client.options[:max_pool_size]).to eq(20)
              end
            end

            context 'when the min_pool_size is equal to the max_pool_size' do

              let(:uri) do
                'mongodb://127.0.0.1:27017/?minPoolSize=10&maxPoolSize=10'
              end

              it 'sets the option' do
                expect(client.options[:min_pool_size]).to eq(10)
                expect(client.options[:max_pool_size]).to eq(10)
              end
            end
          end

          context 'when max_pool_size is not provided' do

            context 'when the min_pool_size is greater than the default max_pool_size' do

              let(:uri) do
                'mongodb://127.0.0.1:27017/?minPoolSize=10'
              end

              it 'raises an Exception' do
                expect {
                  client
                }.to raise_exception(Mongo::Error::InvalidMinPoolSize)
              end
            end

            context 'when the min_pool_size is less than the default max_pool_size' do

              let(:uri) do
                'mongodb://127.0.0.1:27017/?minPoolSize=3'
              end

              it 'sets the option' do
                expect(client.options[:min_pool_size]).to eq(3)
              end
            end

            context 'when the min_pool_size is equal to the max_pool_size' do

              let(:uri) do
                'mongodb://127.0.0.1:27017/?minPoolSize=5'
              end

              it 'sets the option' do
                expect(client.options[:min_pool_size]).to eq(5)
              end
            end
          end
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

      it 'passes the options to the cluster' do
        expect(client.cluster.options[:server_selection_timeout]).to eq(2)
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
        described_class.new([ default_address.to_s ], options)
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

    context 'when the app_name is changed' do

      let(:client) do
        authorized_client
      end

      let!(:original_options) do
        client.options
      end

      let(:new_options) do
        { app_name: 'reports' }
      end

      let!(:new_client) do
        authorized_client.with(new_options)
      end

      it 'returns a new client' do
        expect(new_client).not_to equal(client)
      end

      it 'replaces the existing options' do
        expect(new_client.options).to eq(client.options.merge(new_options))
      end

      it 'does not modify the original client' do
        expect(client.options).to eq(original_options)
      end

      it 'does not keep the same cluster' do
        expect(new_client.cluster).not_to be(client.cluster)
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
        expect(new_client.cluster).to be(client.cluster)
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
