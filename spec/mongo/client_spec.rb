require 'spec_helper'

describe Mongo::Client do

  describe '.new' do
    describe 'options' do
      describe 'read' do
        [
          :primary, :primary_preferred, :secondary, :secondary_preferred,  :nearest
        ].each do |sym|
          it "accepts #{sym} as symbol" do
            client = new_local_client(['127.0.0.1:27017'],
              :read => {:mode => sym})
            # the key got converted to a string here
            expect(client.read_preference).to eq({'mode' => sym})
          end

          # string keys are not documented as being allowed
          # but the code accepts them
          it "accepts #{sym} as string" do
            client = new_local_client(['127.0.0.1:27017'],
              :read => {:mode => sym.to_s})
            # the key got converted to a string here
            # the value remains a string
            expect(client.read_preference).to eq({'mode' => sym.to_s})
          end
        end

        it 'rejects bogus read preference as symbol' do
          expect do
            client = new_local_client(['127.0.0.1:27017'],
              :read => {:mode => :bogus})
          end.to raise_error(Mongo::Error::InvalidReadOption, 'Invalid read option: {"mode"=>:bogus}: mode bogus is not one of recognized modes')
        end

        it 'rejects bogus read preference as string' do
          expect do
            client = new_local_client(['127.0.0.1:27017'],
              :read => {:mode => 'bogus'})
          end.to raise_error(Mongo::Error::InvalidReadOption, 'Invalid read option: {"mode"=>"bogus"}: mode bogus is not one of recognized modes')
        end

        it 'rejects read option specified as a string' do
          expect do
            client = new_local_client(['127.0.0.1:27017'],
              :read => 'primary')
          end.to raise_error(Mongo::Error::InvalidReadOption, 'Invalid read option: primary: must be a hash')
        end

        it 'rejects read option specified as a symbol' do
          expect do
            client = new_local_client(['127.0.0.1:27017'],
              :read => :primary)
          end.to raise_error(Mongo::Error::InvalidReadOption, 'Invalid read option: primary: must be a hash')
        end
      end
    end
  end

  describe '#==' do

    let(:client) do
      new_local_client(
        ['127.0.0.1:27017'],
        :read => { :mode => :primary },
        :database => TEST_DB
      )
    end

    context 'when the other is a client' do

      context 'when the options and cluster are equal' do

        let(:other) do
          new_local_client(
            ['127.0.0.1:27017'],
            :read => { :mode => :primary },
            :database => TEST_DB
          )
        end

        it 'returns true' do
          expect(client).to eq(other)
        end
      end

      context 'when the options are not equal' do

        let(:other) do
          new_local_client(
            ['127.0.0.1:27017'],
            :read => { :mode => :secondary },
            :database => TEST_DB
          )
        end

        it 'returns false' do
          expect(client).not_to eq(other)
        end
      end

      context 'when cluster is not equal' do

        let(:other) do
          new_local_client(
            ['127.0.0.1:27010'],
            :read => { :mode => :primary },
            :database => TEST_DB
          )
        end

        it 'returns false' do
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
      new_local_client(['127.0.0.1:27017'], :database => TEST_DB)
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
      new_local_client(
        ['127.0.0.1:27017'],
        :read => { :mode => :primary },
        :database => TEST_DB
      )
    end

    context 'when the other is a client' do

      context 'when the options and cluster are equal' do

        let(:other) do
          new_local_client(
            ['127.0.0.1:27017'],
            :read => { :mode => :primary },
            :database => TEST_DB
          )
        end

        it 'returns true' do
          expect(client).to eql(other)
        end
      end

      context 'when the options are not equal' do

        let(:other) do
          new_local_client(
            ['127.0.0.1:27017'],
            :read => { :mode => :secondary },
            :database => TEST_DB
          )
        end

        it 'returns false' do
          expect(client).not_to eql(other)
        end
      end

      context 'when the cluster is not equal' do

        let(:other) do
          new_local_client(
            ['127.0.0.1:27010'],
            :read => { :mode => :primary },
            :database => TEST_DB
          )
        end

        it 'returns false' do
          expect(client).not_to eql(other)
        end
      end
    end

    context 'when the other is not a client' do

      let(:client) do
        new_local_client(
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
      new_local_client(
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
      new_local_client(
        ['127.0.0.1:27017'],
        :read => { :mode => :primary },
        :database => TEST_DB
      )
    end

    it 'returns the cluster information' do
      expect(client.inspect).to match(/Cluster(.|\n)*topology=(.|\n)*servers=/)
    end

    context 'when there is sensitive data in the options' do

      let(:client) do
        new_local_client(
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

      context 'when retry_writes is defined' do

        let(:options) do
          { retry_writes: true }
        end

        let(:client) do
          new_local_client([default_address.seed], authorized_client.options.merge(options))
        end

        it 'sets the option' do
          expect(client.options['retry_writes']).to eq(options[:retry_writes])
        end
      end

      context 'when compressors are provided' do

        let(:client) do
          new_local_client([default_address.seed], authorized_client.options.merge(options))
        end

        context 'when the compressor is supported' do

          let(:options) do
            { compressors: ['zlib'] }
          end

          it 'sets the compressor' do
            expect(client.options['compressors']).to eq(options[:compressors])
          end

          it 'sends the compressor in the compression key of the handshake document' do
            expect(client.cluster.app_metadata.send(:document)[:compression]).to eq(options[:compressors])
          end

          it 'uses compression for messages', if: testing_compression? do
            expect(Mongo::Protocol::Compressed).to receive(:new).and_call_original
            client[TEST_COLL].find({}, limit: 1).first
          end

          it 'does not use compression for authentication messages' do
            expect(Mongo::Protocol::Compressed).not_to receive(:new)
            client.cluster.next_primary.send(:with_connection) do |conn|
              conn.send(:authenticate!)
            end
          end
        end

        context 'when the compressor is not supported by the driver' do

          let(:options) do
            { compressors: ['snoopy'] }
          end

          it 'does not set the compressor and warns' do
            expect(Mongo::Logger.logger).to receive(:warn)
            expect(client.options['compressors']).to be_nil
          end

          it 'sets the compression key of the handshake document to an empty array' do
            expect(client.cluster.app_metadata.send(:document)[:compression]).to eq([])
          end

          context 'when one supported compressor and one unsupported compressor are provided', if: compression_enabled? do

            let(:options) do
              { compressors: ['zlib', 'snoopy'] }
            end

            it 'does not set the unsupported compressor and warns' do
              expect(Mongo::Logger.logger).to receive(:warn).at_least(:once)
              expect(client.options['compressors']).to eq(['zlib'])
            end

            it 'sets the compression key of the handshake document to the list of supported compressors' do
              expect(client.cluster.app_metadata.send(:document)[:compression]).to eq(['zlib'])
            end
          end
        end

        context 'when the compressor is not supported by the server', unless: collation_enabled? do

          let(:options) do
            { compressors: ['zlib'] }
          end

          it 'does not set the compressor and warns' do
            expect(Mongo::Logger.logger).to receive(:warn).at_least(:once)
            expect(client.cluster.next_primary.monitor.compressor).to be_nil
          end
        end
      end

      context 'when compressors are not provided', unless: compression_enabled? do

        let(:client) do
          authorized_client
        end

        it 'does not set the compressor' do
          expect(client.options['compressors']).to be_nil
        end

        it 'sets the compression key of the handshake document to an empty array' do
          expect(client.cluster.app_metadata.send(:document)[:compression]).to eq([])
        end

        it 'does not use compression for messages' do
          client[TEST_COLL].find({}, limit: 1).first
          expect(Mongo::Protocol::Compressed).not_to receive(:new)
        end
      end

      context 'when a zlib_compression_level option is provided', if: testing_compression? do

        let(:client) do
          new_local_client([default_address.seed], TEST_OPTIONS.merge(zlib_compression_level: 1))
        end

        it 'sets the option on the client' do
          expect(client.options[:zlib_compression_level]).to eq(1)
        end
      end

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
          new_local_client(['127.0.0.1:27017'], TEST_OPTIONS.merge(options))
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
          new_local_client(['127.0.0.1:27017'], :read => { :mode => :secondary })
        end

        it 'defaults the database to admin' do
          expect(client.database.name).to eq('admin')
        end
      end

      context 'when a database is provided' do

        let(:client) do
          new_local_client(['127.0.0.1:27017'], :database => :testdb)
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

        it 'does not use the global logger' do
          expect(client.cluster.logger).to_not eq(Mongo::Logger.logger)
        end
      end

      context 'when providing a heartbeat_frequency' do

        let(:client) do
          new_local_client(['127.0.0.1:27017'], :heartbeat_frequency => 2)
        end

        it 'sets the heartbeat frequency' do
          expect(client.cluster.options[:heartbeat_frequency]).to eq(client.options[:heartbeat_frequency])
        end
      end

      context 'when min_pool_size is provided' do

        let(:client) do
          new_local_client(['127.0.0.1:27017'], options)
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

      context 'when max_pool_size and min_pool_size are both nil' do

        let(:client) do
          new_local_client(['127.0.0.1:27017'], options)
        end

        let(:options) do
          {
              :min_pool_size => nil,
              :max_pool_size => nil
          }
        end

        it 'does not set either option' do
          expect(client.options[:max_pool_size]).to be_nil
          expect(client.options[:min_pool_size]).to be_nil
        end
      end

      context 'when platform details are specified' do

        let(:app_metadata) do
          client.cluster.app_metadata
        end

        let(:client) do
          new_local_client(['127.0.0.1:27017'], :platform => 'mongoid-6.0.2')
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
          new_local_client(['127.0.0.1:27017'])
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

      context 'when the string uses the SRV Protocol' do
        require_external_connectivity

        let!(:uri) do
          'mongodb+srv://test5.test.build.10gen.cc/testdb'
        end

        let(:client) do
          new_local_client(uri)
        end

        it 'sets the database' do
          expect(client.options[:database]).to eq('testdb')
        end
      end

      context 'when a database is provided' do

        let!(:uri) do
          'mongodb://127.0.0.1:27017/testdb'
        end

        let(:client) do
          new_local_client(uri)
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
          new_local_client(uri)
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
          new_local_client(uri)
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
          new_local_client(uri, :write => { :w => 3 })
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
          new_local_client(uri, :write => { :w => 4 })
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
          new_local_client(uri)
        end

        it 'sets the correct cluster topology' do
          expect(client.cluster.topology).to be_a(Mongo::Cluster::Topology::ReplicaSetNoPrimary)
        end
      end

      context 'when an invalid option is provided' do

        let(:client) do
          new_local_client(['127.0.0.1:27017'], :ssl => false, :invalid => :test)
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

  describe '#server_selector' do

    context 'when there is a read preference set' do

      let(:client) do
        new_local_client(['127.0.0.1:27017'],
                            :database => TEST_DB,
                            :read => mode,
                            :server_selection_timeout => 2)
      end

      let(:server_selector) do
        client.server_selector
      end

      context 'when mode is primary' do

        let(:mode) do
          { :mode => :primary }
        end

        it 'returns a primary server selector' do
          expect(server_selector).to be_a(Mongo::ServerSelector::Primary)
        end

        it 'passes the options to the cluster' do
          expect(client.cluster.options[:server_selection_timeout]).to eq(2)
        end
      end

      context 'when mode is primary_preferred' do

        let(:mode) do
          { :mode => :primary_preferred }
        end

        it 'returns a primary preferred server selector' do
          expect(server_selector).to be_a(Mongo::ServerSelector::PrimaryPreferred)
        end
      end

      context 'when mode is secondary' do

        let(:mode) do
          { :mode => :secondary }
        end

        it 'uses a Secondary server selector' do
          expect(server_selector).to be_a(Mongo::ServerSelector::Secondary)
        end
      end

      context 'when mode is secondary preferred' do

        let(:mode) do
          { :mode => :secondary_preferred }
        end

        it 'uses a Secondary server selector' do
          expect(server_selector).to be_a(Mongo::ServerSelector::SecondaryPreferred)
        end
      end

      context 'when mode is nearest' do

        let(:mode) do
          { :mode => :nearest }
        end

        it 'uses a Secondary server selector' do
          expect(server_selector).to be_a(Mongo::ServerSelector::Nearest)
        end
      end

      context 'when no mode provided' do

        let(:client) do
          new_local_client(['127.0.0.1:27017'],
                              :database => TEST_DB,
                              :server_selection_timeout => 2)
        end

        it 'returns a primary server selector' do
          expect(server_selector).to be_a(Mongo::ServerSelector::Primary)
        end
      end

      context 'when the read preference is printed' do

        let(:client) do
          new_local_client([ default_address.to_s ], options)
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
  end

  describe '#read_preference' do

    let(:client) do
      new_local_client(['127.0.0.1:27017'],
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
        expect(preference).to eq(BSON::Document.new(mode))
      end
    end

    context 'when mode is primary_preferred' do

      let(:mode) do
        { :mode => :primary_preferred }
      end

      it 'returns a primary preferred read preference' do
        expect(preference).to eq(BSON::Document.new(mode))
      end
    end

    context 'when mode is secondary' do

      let(:mode) do
        { :mode => :secondary }
      end

      it 'returns a secondary read preference' do
        expect(preference).to eq(BSON::Document.new(mode))
      end
    end

    context 'when mode is secondary preferred' do

      let(:mode) do
        { :mode => :secondary_preferred }
      end

      it 'returns a secondary preferred read preference' do
        expect(preference).to eq(BSON::Document.new(mode))
      end
    end

    context 'when mode is nearest' do

      let(:mode) do
        { :mode => :nearest }
      end

      it 'returns a nearest read preference' do
        expect(preference).to eq(BSON::Document.new(mode))
      end
    end

    context 'when no mode provided' do

      let(:client) do
        new_local_client(['127.0.0.1:27017'],
                            :database => TEST_DB,
                            :server_selection_timeout => 2)
      end

      it 'returns nil' do
        expect(preference).to be_nil
      end
    end
  end

  describe '#use' do

    let(:client) do
      new_local_client(['127.0.0.1:27017'], :database => TEST_DB)
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
      new_local_client(['127.0.0.1:27017'], :database => TEST_DB)
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
        { app_name: 'client_test' }
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
        new_local_client(
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
        new_local_client(['127.0.0.1:27017'], :write => { :w => 1 }, :database => TEST_DB)
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

    context 'when client is created with ipv6 address' do
      let(:client) do
        new_local_client(['[::1]:27017'], :database => TEST_DB)
      end

      context 'when providing nil' do

        it 'returns the cloned client' do
          expect(client.with(nil)).to eq(client)
        end
      end

      context 'when changing options' do
        let(:new_options) do
          { app_name: 'client_test' }
        end

        let!(:new_client) do
          client.with(new_options)
        end

        it 'returns a new client' do
          expect(new_client).not_to equal(client)
        end
      end
    end
  end

  describe '#write_concern' do

    let(:concern) { client.write_concern }

    context 'when no option was provided to the client' do

      let(:client) { new_local_client(['127.0.0.1:27017'], :database => TEST_DB) }

      it 'does not set the write concern' do
        expect(concern).to be_nil
      end
    end

    context 'when an option is provided' do

      context 'when the option is acknowledged' do

        let(:client) do
          new_local_client(['127.0.0.1:27017'], :write => { :j => true }, :database => TEST_DB)
        end

        it 'returns a acknowledged write concern' do
          expect(concern.get_last_error).to eq(:getlasterror => 1, :j => true)
        end
      end

      context 'when the option is unacknowledged' do

        context 'when the w is 0' do

          let(:client) do
            new_local_client(['127.0.0.1:27017'], :write => { :w => 0 }, :database => TEST_DB)
          end

          it 'returns an unacknowledged write concern' do
            expect(concern.get_last_error).to be_nil
          end
        end

        context 'when the w is -1' do

          let(:client) do
            new_local_client(['127.0.0.1:27017'], :write => { :w => -1 }, :database => TEST_DB)
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

    context 'when filter criteria is present', if: sessions_enabled? do

      let(:result) do
        root_authorized_client.database_names(filter)
      end

      let(:filter) do
        { name: TEST_DB }
      end

      it 'returns a filtered list of database names' do
        expect(result.length).to eq(1)
        expect(result.first).to eq(filter[:name])
      end
    end
  end

  describe '#list_databases' do

    it 'returns a list of database info documents' do
      expect(
        root_authorized_client.list_databases.collect do |i|
          i['name']
        end).to include('admin')
    end

    context 'when filter criteria is present', if: sessions_enabled? do

      let(:result) do
        root_authorized_client.list_databases(filter)
      end

      let(:filter) do
        { name: TEST_DB }
      end

      it 'returns a filtered list of database info documents' do
        expect(result.length).to eq(1)
        expect(result[0]['name']).to eq(filter[:name])
      end
    end

    context 'when name_only is true' do

      let(:client_options) do
        root_authorized_client.options.merge(heartbeat_frequency: 100, monitoring: true)
      end

      let(:client) do
        Mongo::Client.new(SpecConfig.instance.addresses, client_options).tap do |cl|
          cl.subscribe(Mongo::Monitoring::COMMAND, EventSubscriber.clear_events!)
        end
      end

      let(:command) do
        EventSubscriber.started_events.find { |c| c.command_name == 'listDatabases' }.command
      end

      before do
        client.list_databases({}, true)
      end

      it 'sends the command with the nameOnly flag set to true' do
        expect(command[:nameOnly]).to be(true)
      end
    end
  end

  describe '#list_mongo_databases' do

    let(:options) do
      { read: { mode: :secondary } }
    end

    let(:client) do
      root_authorized_client.with(options)
    end

    let(:result) do
      client.list_mongo_databases
    end

    it 'returns a list of Mongo::Database objects' do
      expect(result).to all(be_a(Mongo::Database))
    end

    it 'creates database with specified options' do
      expect(result.first.options[:read]).to eq(BSON::Document.new(options)[:read])
    end

    context 'when filter criteria is present', if: sessions_enabled? do

      let(:result) do
        client.list_mongo_databases(filter)
      end

      let(:filter) do
        { name: TEST_DB }
      end

      it 'returns a filtered list of Mongo::Database objects' do
        expect(result.length).to eq(1)
        expect(result.first.name).to eq(filter[:name])
      end
    end
  end

  describe '#close' do

    let(:client) do
      new_local_client(['127.0.0.1:27017'])
    end

    before do
      # note that disconnect! is called also in the after block
      expect(client.cluster).to receive(:disconnect!).twice.and_call_original
    end

    it 'disconnects the cluster and returns true' do
      expect(client.close).to be(true)
    end
  end

  describe '#reconnect' do

    let(:client) do
      new_local_client(['127.0.0.1:27017'])
    end

    it 'replaces the cluster' do
      old_id = client.cluster.object_id
      client.reconnect
      new_id = client.cluster.object_id
      expect(new_id).not_to eql(old_id)
    end

    it 'returns true' do
      expect(client.reconnect).to be(true)
    end
  end

  describe '#dup' do

    let(:client) do
      new_local_client(
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

  describe '#start_session' do

    let(:session) do
      authorized_client.start_session
    end

    context 'when sessions are supported', if: test_sessions? do

      it 'creates a session' do
        expect(session).to be_a(Mongo::Session)
      end

      it 'sets the last use field to the current time', retry: 4 do
        expect(session.instance_variable_get(:@server_session).last_use).to be_within(1).of(Time.now)
      end

      context 'when options are provided' do

        let(:options) do
          { causal_consistency: true }
        end

        let(:session) do
          authorized_client.start_session(options)
        end

        it 'sets the options on the session' do
          expect(session.options[:causal_consistency]).to eq(options[:causal_consistency])
        end
      end

      context 'when options are not provided' do

        it 'does not set options on the session' do
          expect(session.options).to eq({ implicit: false })
        end
      end

      context 'when a session is checked out and checked back in' do

        let!(:session_a) do
          authorized_client.start_session
        end

        let!(:session_b) do
          authorized_client.start_session
        end

        let!(:session_a_server_session) do
          session_a.instance_variable_get(:@server_session)
        end

        let!(:session_b_server_session) do
          session_b.instance_variable_get(:@server_session)
        end

        before do
          session_a_server_session.next_txn_num
          session_a_server_session.next_txn_num
          session_b_server_session.next_txn_num
          session_b_server_session.next_txn_num
          session_a.end_session
          session_b.end_session
        end

        it 'is returned to the front of the queue' do
          expect(authorized_client.start_session.instance_variable_get(:@server_session)).to be(session_b_server_session)
          expect(authorized_client.start_session.instance_variable_get(:@server_session)).to be(session_a_server_session)
        end

        it 'preserves the transaction numbers on the server sessions' do
          expect(authorized_client.start_session.next_txn_num).to be(3)
          expect(authorized_client.start_session.next_txn_num).to be(3)
        end
      end

      context 'when an implicit session is used' do

        before do
          authorized_client.database.command(ping: 1)
        end

        let(:pool) do
          authorized_client.cluster.session_pool
        end

        let!(:before_last_use) do
          pool.instance_variable_get(:@queue)[0].last_use
        end

        it 'uses the session and updates the last use time' do
          authorized_client.database.command(ping: 1)
          expect(before_last_use).to be < (pool.instance_variable_get(:@queue)[0].last_use)
        end
      end
    end

    context 'when two clients have the same cluster', if: test_sessions? do

      let(:client) do
        authorized_client.with(read: { mode: :secondary })
      end

      let(:session) do
        authorized_client.start_session
      end

      it 'allows the session to be used across the clients' do
        client[TEST_COLL].insert_one({ a: 1 }, session: session)
      end
    end

    context 'when two clients have different clusters', if: test_sessions? do

      let(:client) do
        authorized_client_with_retry_writes
      end

      let(:session) do
        authorized_client.start_session
      end

      it 'raises an exception' do
        expect {
          client[TEST_COLL].insert_one({ a: 1 }, session: session)
        }.to raise_exception(Mongo::Error::InvalidSession)
      end
    end

    context 'when sessions are not supported', unless: sessions_enabled? do

      it 'raises an exception' do
        expect {
          session
        }.to raise_exception(Mongo::Error::InvalidSession)
      end
    end
  end
end
