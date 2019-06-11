require 'spec_helper'

describe Mongo::Client do
  before do
    ClientRegistry.instance.close_all_clients
  end

  describe '.new' do
    describe 'options' do
      describe 'read' do
        [
          :primary, :primary_preferred, :secondary, :secondary_preferred,  :nearest
        ].each do |sym|
          it "accepts #{sym} as symbol" do
            client = new_local_client_nmio(['127.0.0.1:27017'],
              :read => {:mode => sym})
            # the key got converted to a string here
            expect(client.read_preference).to eq({'mode' => sym})
          end

          # string keys are not documented as being allowed
          # but the code accepts them
          it "accepts #{sym} as string" do
            client = new_local_client_nmio(['127.0.0.1:27017'],
              :read => {:mode => sym.to_s})
            # the key got converted to a string here
            # the value remains a string
            expect(client.read_preference).to eq({'mode' => sym.to_s})
          end
        end

        context 'when not linting' do
          skip_if_linting

          it 'rejects bogus read preference as symbol' do
            expect do
              client = new_local_client_nmio(['127.0.0.1:27017'],
                :read => {:mode => :bogus})
            end.to raise_error(Mongo::Error::InvalidReadOption, 'Invalid read option: {"mode"=>:bogus}: mode bogus is not one of recognized modes')
          end

          it 'rejects bogus read preference as string' do
            expect do
              client = new_local_client_nmio(['127.0.0.1:27017'],
                :read => {:mode => 'bogus'})
            end.to raise_error(Mongo::Error::InvalidReadOption, 'Invalid read option: {"mode"=>"bogus"}: mode bogus is not one of recognized modes')
          end

          it 'rejects read option specified as a string' do
            expect do
              client = new_local_client_nmio(['127.0.0.1:27017'],
                :read => 'primary')
            end.to raise_error(Mongo::Error::InvalidReadOption, 'Invalid read option: primary: must be a hash')
          end

          it 'rejects read option specified as a symbol' do
            expect do
              client = new_local_client_nmio(['127.0.0.1:27017'],
                :read => :primary)
            end.to raise_error(Mongo::Error::InvalidReadOption, 'Invalid read option: primary: must be a hash')
          end
        end
      end
    end

    context 'with scan: false' do
      it 'does not perform i/o' do
        allow_any_instance_of(Mongo::Server::Monitor).to receive(:run!)
        expect_any_instance_of(Mongo::Server::Monitor).not_to receive(:scan!)
        start_time = Time.now
        # return should be instant
        c = Timeout.timeout(1) do
          ClientRegistry.instance.new_local_client(['1.1.1.1'], scan: false)
        end
        expect(c.cluster.servers).to be_empty
        c.close
      end
    end

    context 'with default scan: true' do
      # TODO this test requires there being no outstanding background
      # monitoring threads running, as otherwise the scan! expectation
      # can be executed on a thread that belongs to one of the global
      # clients for instance
      it 'performs one round of sdam' do
        # Does not work due to
        # https://github.com/rspec/rspec-mocks/issues/1242.
        #expect_any_instance_of(Mongo::Server::Monitor).to receive(:scan!).
        #  exactly(SpecConfig.instance.addresses.length).times.and_call_original
        c = ClientRegistry.instance.new_local_client(
          SpecConfig.instance.addresses, SpecConfig.instance.test_options)
        expect(c.cluster.servers).not_to be_empty
        c.close
      end

      # This checks the case of all initial seeds being removed from
      # cluster during SDAM
      context 'me mismatch on the only initial seed' do
        let(:address) do
          address = SpecConfig.instance.addresses.first
          port = address.sub(/^.*:/, '').to_i
          address = address.sub(/:.*/, '')
          case address
          when '127.0.0.1'
            'localhost'
          when /^(\d+\.){3}\d+$/
            skip 'This test requires a hostname or 127.0.0.1 as address'
          else
            # We don't know if mongod is listening on ipv4 or ipv6,
            # in principle.
            # Our tests use ipv4, so hardcode that for now.
            # To support both we need to try both addresses
            # which will make this test more complicated.
            resolved_address = Addrinfo.getaddrinfo(address, port, Socket::PF_INET).first.ip_address
            if resolved_address.include?(':')
              "[#{resolved_address}]"
            else
              resolved_address
            end + ":#{port}"
          end
        end

        let(:client) do
          ClientRegistry.instance.new_local_client(
            [address],
            # Specify server selection timeout here because test suite sets
            # one by default and it's fairly low
            SpecConfig.instance.test_options.merge(server_selection_timeout: 5))
        end

        it 'does not wait for server selection timeout' do
          start_time = Time.now
          client
          time_taken = Time.now - start_time
          expect(time_taken < 3).to be true

          # run a command to ensure the client is a working one
          client.database.command(ismaster: 1)
        end
      end
    end

    context 'with monitoring_io: false' do
      let(:client) do
        new_local_client(['127.0.0.1:27017'], monitoring_io: false)
      end

      it 'passes monitoring_io: false to cluster' do
        expect(client.cluster.options[:monitoring_io]).to be false
      end
    end
  end

  describe '#initialize' do

    context 'when providing options' do

      context 'retry_writes option' do
        let(:client) do
          new_local_client_nmio(SpecConfig.instance.addresses, options)
        end

        context 'when retry_writes is true' do

          let(:options) do
            { retry_writes: true }
          end

          it 'sets retry_writes to true' do
            expect(client.options['retry_writes']).to be true
          end
        end

        context 'when retry_writes is false' do

          let(:options) do
            { retry_writes: false }
          end

          it 'sets retry_writes to false' do
            expect(client.options['retry_writes']).to be false
          end
        end

        context 'when retry_writes is not given' do

          let(:options) do
            { }
          end

          it 'sets retry_writes to true' do
            expect(client.options['retry_writes']).to be true
          end
        end
      end

      context 'when compressors are provided' do

        let(:client) do
          new_local_client(SpecConfig.instance.addresses, authorized_client.options.merge(options))
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

          context 'when server supports compression' do
            require_compression
            min_server_fcv '3.6'

            it 'uses compression for messages' do
              expect(Mongo::Protocol::Compressed).to receive(:new).and_call_original
              client[TEST_COLL].find({}, limit: 1).first
            end
          end

          it 'does not use compression for authentication messages' do
            expect(Mongo::Protocol::Compressed).not_to receive(:new)
            client.cluster.next_primary.send(:with_connection) do |conn|
              conn.send(:authenticate!, conn)
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

          context 'when one supported compressor and one unsupported compressor are provided' do
            require_compression
            min_server_fcv '3.6'

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

        context 'when the compressor is not supported by the server' do
          max_server_version '3.4'

          let(:options) do
            { compressors: ['zlib'] }
          end

          it 'does not set the compressor and warns' do
            expect(Mongo::Logger.logger).to receive(:warn).at_least(:once)
            expect(client.cluster.next_primary.monitor.compressor).to be_nil
          end
        end
      end

      context 'when compressors are not provided' do
        require_no_compression

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

      context 'when a zlib_compression_level option is provided' do
        require_compression
        min_server_fcv '3.6'

        let(:client) do
          new_local_client_nmio(SpecConfig.instance.addresses, SpecConfig.instance.test_options.merge(zlib_compression_level: 1))
        end

        it 'sets the option on the client' do
          expect(client.options[:zlib_compression_level]).to eq(1)
        end
      end

      context 'when ssl options are provided' do

        let(:options) do
          {
            :ssl => true,
            :ssl_ca_cert => SpecConfig.instance.ca_cert_path,
            :ssl_ca_cert_string => 'ca cert string',
            :ssl_ca_cert_object => 'ca cert object',
            :ssl_cert => SpecConfig.instance.client_cert_path,
            :ssl_cert_string => 'cert string',
            :ssl_cert_object => 'cert object',
            :ssl_key => SpecConfig.instance.client_key_path,
            :ssl_key_string => 'key string',
            :ssl_key_object => 'key object',
            :ssl_key_pass_phrase => 'passphrase',
            :ssl_verify => true
          }
        end

        let(:client) do
          new_local_client_nmio(['127.0.0.1:27017'], options)
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
          new_local_client_nmio(['127.0.0.1:27017'], :read => { :mode => :secondary })
        end

        it 'defaults the database to admin' do
          expect(client.database.name).to eq('admin')
        end
      end

      context 'when a database is provided' do

        let(:client) do
          new_local_client_nmio(['127.0.0.1:27017'], :database => :testdb)
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
          new_local_client_nmio(['127.0.0.1:27017'], :heartbeat_frequency => 2)
        end

        it 'sets the heartbeat frequency' do
          expect(client.cluster.options[:heartbeat_frequency]).to eq(client.options[:heartbeat_frequency])
        end
      end

      context 'when min_pool_size is provided' do

        let(:client) do
          new_local_client_nmio(['127.0.0.1:27017'], options)
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
                :min_pool_size => Mongo::Server::ConnectionPool::DEFAULT_MAX_SIZE
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
          new_local_client_nmio(['127.0.0.1:27017'], options)
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
          new_local_client_nmio(['127.0.0.1:27017'], :platform => 'mongoid-6.0.2')
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
          new_local_client_nmio(['127.0.0.1:27017'])
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
          new_local_client_nmio(uri)
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
          new_local_client_nmio(uri)
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
          new_local_client_nmio(uri)
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
          new_local_client_nmio(uri)
        end

        let(:expected_options) do
          Mongo::Options::Redacted.new(:write => { :w => 3 },
            monitoring_io: false, :database => 'testdb', retry_writes: true,
            retry_reads: true)
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
          new_local_client_nmio(uri, :write => { :w => 3 })
        end

        let(:expected_options) do
          Mongo::Options::Redacted.new(:write => { :w => 3 },
            monitoring_io: false, :database => 'testdb', retry_writes: true,
            retry_reads: true)
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
          new_local_client_nmio(uri, :write => { :w => 4 })
        end

        let(:expected_options) do
          Mongo::Options::Redacted.new(:write => { :w => 4 },
            monitoring_io: false, :database => 'testdb', retry_writes: true,
            retry_reads: true)
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
          new_local_client_nmio(uri)
        end

        it 'sets the correct cluster topology' do
          expect(client.cluster.topology).to be_a(Mongo::Cluster::Topology::ReplicaSetNoPrimary)
        end
      end

      context 'when an invalid option is provided' do

        let(:client) do
          new_local_client_nmio(['127.0.0.1:27017'], :ssl => false, :invalid => :test)
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

  shared_examples_for 'duplicated client with duplicated monitoring' do
    let(:monitoring) { client.send(:monitoring) }
    let(:new_monitoring) { new_client.send(:monitoring) }

    it 'duplicates monitoring' do
      expect(new_monitoring).not_to eql(monitoring)
    end

    it 'copies monitoring subscribers' do
      monitoring.subscribers.clear
      client.subscribe(Mongo::Monitoring::SERVER_HEARTBEAT, EventSubscriber.new)
      expect(monitoring.present_subscribers.length).to eq(1)
      expect(monitoring.subscribers[Mongo::Monitoring::SERVER_HEARTBEAT].length).to eq(1)

      # this duplicates the client
      expect(new_monitoring.present_subscribers.length).to eq(1)
      expect(new_monitoring.subscribers[Mongo::Monitoring::SERVER_HEARTBEAT].length).to eq(1)
    end

    it 'does not change subscribers on original client' do
      monitoring.subscribers.clear
      client.subscribe(Mongo::Monitoring::SERVER_HEARTBEAT, EventSubscriber.new)
      expect(monitoring.present_subscribers.length).to eq(1)
      expect(monitoring.subscribers[Mongo::Monitoring::SERVER_HEARTBEAT].length).to eq(1)

      new_client.subscribe(Mongo::Monitoring::SERVER_HEARTBEAT, EventSubscriber.new)
      new_client.subscribe(Mongo::Monitoring::SERVER_HEARTBEAT, EventSubscriber.new)
      expect(new_monitoring.present_subscribers.length).to eq(1)
      expect(new_monitoring.subscribers[Mongo::Monitoring::SERVER_HEARTBEAT].length).to eq(3)
      # original client should not have gotten any of the new subscribers
      expect(monitoring.present_subscribers.length).to eq(1)
      expect(monitoring.subscribers[Mongo::Monitoring::SERVER_HEARTBEAT].length).to eq(1)
    end
  end

  shared_examples_for 'duplicated client with reused monitoring' do
    let(:monitoring) { client.send(:monitoring) }
    let(:new_monitoring) { new_client.send(:monitoring) }

    it 'reuses monitoring' do
      expect(new_monitoring).to eql(monitoring)
    end
  end

  shared_examples_for 'duplicated client with clean slate monitoring' do
    let(:monitoring) { client.send(:monitoring) }
    let(:new_monitoring) { new_client.send(:monitoring) }

    it 'does not reuse monitoring' do
      expect(new_monitoring).not_to eql(monitoring)
    end

    it 'resets monitoring subscribers' do
      monitoring.subscribers.clear
      client.subscribe(Mongo::Monitoring::SERVER_HEARTBEAT, EventSubscriber.new)
      expect(monitoring.present_subscribers.length).to eq(1)
      expect(monitoring.subscribers[Mongo::Monitoring::SERVER_HEARTBEAT].length).to eq(1)

      # this duplicates the client
      # 7 is how many subscribers driver sets up by default
      expect(new_monitoring.present_subscribers.length).to eq(7)
      # ... none of which are for heartbeats
      expect(new_monitoring.subscribers[Mongo::Monitoring::SERVER_HEARTBEAT].length).to eq(0)
    end

    it 'does not change subscribers on original client' do
      monitoring.subscribers.clear
      client.subscribe(Mongo::Monitoring::SERVER_HEARTBEAT, EventSubscriber.new)
      expect(monitoring.present_subscribers.length).to eq(1)
      expect(monitoring.subscribers[Mongo::Monitoring::SERVER_HEARTBEAT].length).to eq(1)

      new_client.subscribe(Mongo::Monitoring::SERVER_HEARTBEAT, EventSubscriber.new)
      new_client.subscribe(Mongo::Monitoring::SERVER_HEARTBEAT, EventSubscriber.new)
      # 7 default subscribers + heartbeat
      expect(new_monitoring.present_subscribers.length).to eq(8)
      # the heartbeat subscriber on the original client is not inherited
      expect(new_monitoring.subscribers[Mongo::Monitoring::SERVER_HEARTBEAT].length).to eq(2)
      # original client should not have gotten any of the new subscribers
      expect(monitoring.present_subscribers.length).to eq(1)
      expect(monitoring.subscribers[Mongo::Monitoring::SERVER_HEARTBEAT].length).to eq(1)
    end
  end

  describe '#use' do

    let(:client) do
      new_local_client_nmio(['127.0.0.1:27017'], :database => SpecConfig.instance.test_db)
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
      new_local_client_nmio(['127.0.0.1:27017'], :database => SpecConfig.instance.test_db)
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
        new_local_client_nmio(
          ['127.0.0.1:27017'],
          :read => { :mode => :secondary }, :write => { :w => 1 }, :database => SpecConfig.instance.test_db
        )
      end

      let!(:new_client) do
        client.with(:read => { :mode => :primary })
      end

      let(:new_options) do
        Mongo::Options::Redacted.new(:read => { :mode => :primary },
          :write => { :w => 1 }, monitoring_io: false,
          :database => SpecConfig.instance.test_db, retry_writes: true, retry_reads: true)
      end

      let(:original_options) do
        Mongo::Options::Redacted.new(:read => { :mode => :secondary },
          :write => { :w => 1 }, monitoring_io: false,
          :database => SpecConfig.instance.test_db, retry_writes: true, retry_reads: true)
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
        new_local_client(['127.0.0.1:27017'], :write => { :w => 1 }, :database => SpecConfig.instance.test_db)
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
        new_local_client_nmio(['[::1]:27017'], :database => SpecConfig.instance.test_db)
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

    context 'when new client has a new cluster' do
      let(:client) do
        new_local_client(['127.0.0.1:27017'], :database => SpecConfig.instance.test_db)
      end
      let(:new_client) do
        client.with(app_name: 'client_construction_spec').tap do |new_client|
          expect(new_client.cluster).not_to eql(client.cluster)
        end
      end

      it_behaves_like 'duplicated client with clean slate monitoring'
    end

    context 'when new client shares cluster with original client' do
      let(:new_client) do
        client.with(database: 'client_construction_spec').tap do |new_client|
          expect(new_client.cluster).to eql(client.cluster)
        end
      end

      it_behaves_like 'duplicated client with reused monitoring'
    end

    # Since we either reuse monitoring or reset it to a clean slate
    # in #with, the consistent behavior is to never transfer sdam_proc to
    # the new client.
    context 'when sdam_proc is given on original client' do
      let(:subscriber) { EventSubscriber.new }

      let(:sdam_proc) do
        Proc.new do |client|
          client.subscribe(Mongo::Monitoring::SERVER_HEARTBEAT, subscriber)
        end
      end

      let(:new_client) { client.with(database: 'foo') }

      it 'does not copy sdam_proc option to new client' do
        client = new_local_client(['a'], sdam_proc: sdam_proc)
        expect(new_client.options[:sdam_proc]).to be nil
      end

      it 'does not notify subscribers set up by sdam_proc' do
        client = new_local_client(['a'], sdam_proc: sdam_proc)
        expect(subscriber.started_events.length).to eq 1
        subscriber.started_events.clear

        new_client
        expect(subscriber.started_events.length).to eq 0
      end
    end
  end

  describe '#dup' do

    let(:client) do
      new_local_client(
          ['127.0.0.1:27017'],
          :read => { :mode => :primary },
          :database => SpecConfig.instance.test_db
      )
    end

    let(:new_client) { client.dup }

    it 'creates a client with Redacted options' do
      expect(new_client.options).to be_a(Mongo::Options::Redacted)
    end

    it_behaves_like 'duplicated client with reused monitoring'
  end
end
