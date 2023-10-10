# frozen_string_literal: true

require 'spec_helper'

SINGLE_CLIENT = [ '127.0.0.1:27017' ].freeze

# let these existing styles stand, rather than going in for a deep refactoring
# of these specs.
#
# possible future work: re-enable these one at a time and do the hard work of
# making them right.
#
# rubocop:disable RSpec/ExpectInHook, RSpec/MessageSpies, RSpec/ExampleLength
# rubocop:disable RSpec/ContextWording, RSpec/RepeatedExampleGroupDescription
# rubocop:disable RSpec/ExampleWording, Style/BlockComments, RSpec/AnyInstance
# rubocop:disable RSpec/VerifiedDoubles
describe Mongo::Client do
  clean_slate

  let(:subscriber) { Mrss::EventSubscriber.new }

  describe '.new' do
    context 'with scan: false' do
      fails_on_jruby

      it 'does not perform i/o' do
        allow_any_instance_of(Mongo::Server::Monitor).to receive(:run!)
        expect_any_instance_of(Mongo::Server::Monitor).not_to receive(:scan!)

        # return should be instant
        c = Timeout.timeout(1) do
          ClientRegistry.instance.new_local_client([ '1.1.1.1' ], scan: false)
        end
        expect(c.cluster.servers).to be_empty
        c.close
      end
    end

    context 'with default scan: true' do
      shared_examples 'does not wait for server selection timeout' do
        let(:logger) do
          Logger.new($stdout, level: Logger::DEBUG)
        end

        let(:subscriber) do
          Mongo::Monitoring::UnifiedSdamLogSubscriber.new(
            logger: logger,
            log_prefix: 'CCS-SDAM'
          )
        end

        let(:client) do
          ClientRegistry.instance.new_local_client(
            [ address ],
            # Specify server selection timeout here because test suite sets
            # one by default and it's fairly low
            SpecConfig.instance.test_options.merge(
              connect_timeout: 1,
              socket_timeout: 1,
              server_selection_timeout: 8,
              logger: logger,
              log_prefix: 'CCS-CLIENT',
              sdam_proc: ->(client) { subscriber.subscribe(client) }
            )
          )
        end

        it 'does not wait for server selection timeout' do
          time_taken = Benchmark.realtime do
            # Client is created here.
            client
          end
          puts "client_construction_spec.rb: Cluster is: #{client.cluster.summary}"

          # Because the first round of sdam waits for server statuses to change
          # rather than for server selection semaphore on the cluster which
          # is signaled after topology is updated, the topology here could be
          # old (i.e. a monitor thread was just about to update the topology
          # but hasn't quite gotten to it. Add a small delay to compensate.
          # This issue doesn't apply to real applications which will wait for
          # server selection semaphore.
          sleep 0.1

          actual_class = client.cluster.topology.class
          expect([
                   Mongo::Cluster::Topology::ReplicaSetWithPrimary,
                   Mongo::Cluster::Topology::Single,
                   Mongo::Cluster::Topology::Sharded,
                   Mongo::Cluster::Topology::LoadBalanced,
                 ]).to include(actual_class)
          expect(time_taken).to be < 5

          # run a command to ensure the client is a working one
          client.database.command(ping: 1)
        end
      end

      context 'when cluster is monitored' do
        require_topology :single, :replica_set, :sharded

        # TODO: this test requires there being no outstanding background
        # monitoring threads running, as otherwise the scan! expectation
        # can be executed on a thread that belongs to one of the global
        # clients for instance
        it 'performs one round of sdam' do
          # Does not work due to
          # https://github.com/rspec/rspec-mocks/issues/1242.
          #
          # expect_any_instance_of(Mongo::Server::Monitor).to receive(:scan!).
          #  exactly(SpecConfig.instance.addresses.length).times.and_call_original
          c = new_local_client(SpecConfig.instance.addresses, SpecConfig.instance.test_options)
          expect(c.cluster.servers).not_to be_empty
        end

        # This checks the case of all initial seeds being removed from
        # cluster during SDAM
        context 'me mismatch on the only initial seed' do
          let(:address) do
            ClusterConfig.instance.alternate_address.to_s
          end

          include_examples 'does not wait for server selection timeout'
        end
      end

      context 'when cluster is not monitored' do
        require_topology :load_balanced

        let(:address) do
          ClusterConfig.instance.alternate_address.to_s
        end

        include_examples 'does not wait for server selection timeout'
      end
    end

    context 'with monitoring_io: false' do
      let(:client) do
        new_local_client(SINGLE_CLIENT, monitoring_io: false)
      end

      it 'passes monitoring_io: false to cluster' do
        expect(client.cluster.options[:monitoring_io]).to be false
      end
    end
  end

  describe '#initialize' do
    context 'when providing options' do
      context 'with auto_encryption_options' do
        require_libmongocrypt

        include_context 'define shared FLE helpers'

        let(:client) do
          new_local_client_nmio(
            SpecConfig.instance.addresses,
            SpecConfig.instance.test_options.merge(client_opts)
          )
        end

        let(:client_opts) { { auto_encryption_options: auto_encryption_options } }

        let(:auto_encryption_options) do
          {
            key_vault_client: key_vault_client,
            key_vault_namespace: key_vault_namespace,
            kms_providers: kms_providers,
            schema_map: schema_map,
            bypass_auto_encryption: bypass_auto_encryption,
            extra_options: extra_options,
          }
        end

        let(:key_vault_client) { new_local_client_nmio(SpecConfig.instance.addresses) }

        let(:bypass_auto_encryption) { true }

        let(:extra_options) do
          {
            mongocryptd_uri: mongocryptd_uri,
            mongocryptd_bypass_spawn: mongocryptd_bypass_spawn,
            mongocryptd_spawn_path: mongocryptd_spawn_path,
            mongocryptd_spawn_args: mongocryptd_spawn_args,
          }
        end

        let(:mongocryptd_uri) { 'mongodb://localhost:27021' }
        let(:mongocryptd_bypass_spawn) { true }
        let(:mongocryptd_spawn_path) { '/spawn/path' }
        let(:mongocryptd_spawn_args) { [ '--idleShutdownTimeoutSecs=100' ] }

        shared_examples 'a functioning auto encryption client' do
          let(:encryption_options) { client.encrypter.options }

          context 'when auto_encrypt_opts are nil' do
            let(:auto_encryption_options) { nil }

            it 'does not raise an exception' do
              expect { client }.not_to raise_error
            end
          end

          context 'when key_vault_namespace is nil' do
            let(:key_vault_namespace) { nil }

            it 'raises an exception' do
              expect { client }.to raise_error(ArgumentError, /key_vault_namespace option cannot be nil/)
            end
          end

          context 'when key_vault_namespace is incorrectly formatted' do
            let(:key_vault_namespace) { 'not.good.formatting' }

            it 'raises an exception' do
              expect { client }.to raise_error(
                ArgumentError,
                /key_vault_namespace option must be in the format database.collection/
              )
            end
          end

          context 'when kms_providers is nil' do
            let(:kms_providers) { nil }

            it 'raises an exception' do
              expect { client }.to raise_error(ArgumentError, /KMS providers options must not be nil/)
            end
          end

          context 'when kms_providers doesn\'t have local or aws keys' do
            let(:kms_providers) { { random_key: 'hello' } }

            it 'raises an exception' do
              expect { client }.to raise_error(
                ArgumentError,
                /KMS providers options must have one of the following keys: :aws, :azure, :gcp, :kmip, :local/
              )
            end
          end

          context 'when local kms_provider is incorrectly formatted' do
            let(:kms_providers) { { local: { wrong_key: 'hello' } } }

            it 'raises an exception' do
              expect { client }.to raise_error(
                ArgumentError,
                /Local KMS provider options must be in the format: { key: 'MASTER-KEY' }/
              )
            end
          end

          context 'when aws kms_provider is incorrectly formatted' do
            let(:kms_providers) { { aws: { wrong_key: 'hello' } } }

            let(:expected_options_format) do
              "{ access_key_id: 'YOUR-ACCESS-KEY-ID', secret_access_key: 'SECRET-ACCESS-KEY' }"
            end

            it 'raises an exception' do
              expect { client }.to raise_error(
                ArgumentError,
                / AWS KMS provider options must be in the format: #{expected_options_format}/
              )
            end
          end

          context 'with an invalid schema map' do
            let(:schema_map) { '' }

            it 'raises an exception' do
              expect { client }.to raise_error(ArgumentError, /schema_map must be a Hash or nil/)
            end
          end

          context 'with valid options' do
            it 'does not raise an exception' do
              expect { client }.not_to raise_error
            end

            context 'with a nil schema_map' do
              let(:schema_map) { nil }

              it 'does not raise an exception' do
                expect { client }.not_to raise_error
              end
            end

            it 'sets options on the client' do
              expect(encryption_options[:key_vault_client]).to eq(key_vault_client)
              expect(encryption_options[:key_vault_namespace]).to eq(key_vault_namespace)
              # Don't explicitly expect kms_providers to avoid accidentally exposing
              # sensitive data in evergreen logs
              expect(encryption_options[:kms_providers]).to be_a(Hash)
              expect(encryption_options[:schema_map]).to eq(schema_map)
              expect(encryption_options[:bypass_auto_encryption]).to eq(bypass_auto_encryption)
              expect(encryption_options[:extra_options][:mongocryptd_uri]).to eq(mongocryptd_uri)
              expect(encryption_options[:extra_options][:mongocryptd_bypass_spawn]).to eq(mongocryptd_bypass_spawn)
              expect(encryption_options[:extra_options][:mongocryptd_spawn_path]).to eq(mongocryptd_spawn_path)
              expect(encryption_options[:extra_options][:mongocryptd_spawn_args]).to eq(mongocryptd_spawn_args)

              expect(client.encrypter.mongocryptd_client.options[:monitoring_io]).to be false
            end

            context 'with default extra options' do
              let(:auto_encryption_options) do
                {
                  key_vault_namespace: key_vault_namespace,
                  kms_providers: kms_providers,
                  schema_map: schema_map,
                }
              end

              it 'sets key_vault_client with no encryption options' do
                key_vault_client = client.encrypter.key_vault_client
                expect(key_vault_client.options['auto_encryption_options']).to be_nil
              end

              it 'sets bypass_auto_encryption to false' do
                expect(encryption_options[:bypass_auto_encryption]).to be false
              end

              it 'sets extra options to defaults' do
                expect(encryption_options[:extra_options][:mongocryptd_uri]).to eq('mongodb://localhost:27020')
                expect(encryption_options[:extra_options][:mongocryptd_bypass_spawn]).to be false
                expect(encryption_options[:extra_options][:mongocryptd_spawn_path]).to eq('mongocryptd')
                expect(encryption_options[:extra_options][:mongocryptd_spawn_args])
                  .to eq([ '--idleShutdownTimeoutSecs=60' ])
              end
            end

            context 'with mongocryptd_spawn_args that don\'t include idleShutdownTimeoutSecs' do
              let(:mongocryptd_spawn_args) { [ '--otherArgument=true' ] }

              it 'adds a default value to mongocryptd_spawn_args' do
                expect(encryption_options[:extra_options][:mongocryptd_spawn_args])
                  .to eq(mongocryptd_spawn_args + [ '--idleShutdownTimeoutSecs=60' ])
              end
            end

            context 'with mongocryptd_spawn_args that has idleShutdownTimeoutSecs as two arguments' do
              let(:mongocryptd_spawn_args) { [ '--idleShutdownTimeoutSecs', 100 ] }

              it 'does not modify mongocryptd_spawn_args' do
                expect(encryption_options[:extra_options][:mongocryptd_spawn_args]).to eq(mongocryptd_spawn_args)
              end
            end

            context 'with default key_vault_client' do
              let(:key_vault_client) { nil }

              it 'creates a key_vault_client' do
                key_vault_client = encryption_options[:key_vault_client]

                expect(key_vault_client).to be_a(described_class)
              end
            end
          end
        end

        context 'with AWS KMS providers' do
          include_context 'with AWS kms_providers' do
            it_behaves_like 'a functioning auto encryption client'
          end
        end

        context 'with local KMS providers' do
          include_context 'with local kms_providers' do
            it_behaves_like 'a functioning auto encryption client'
          end
        end
      end

      context 'timeout options' do
        let(:client) do
          new_local_client(
            SpecConfig.instance.addresses,
            SpecConfig.instance.authorized_test_options.merge(options)
          )
        end

        context 'when network timeouts are zero' do
          let(:options) { { socket_timeout: 0, connect_timeout: 0 } }

          it 'sets options to zeros' do
            expect(client.options[:socket_timeout]).to be == 0
            expect(client.options[:connect_timeout]).to be == 0
          end

          it 'connects and performs operations successfully' do
            expect { client.database.command(ping: 1) }
              .not_to raise_error
          end
        end

        %i[ socket_timeout connect_timeout ].each do |option|
          context "when #{option} is negative" do
            let(:options) { { option => -1 } }

            it 'fails client creation' do
              expect { client }
                .to raise_error(ArgumentError, /#{option} must be a non-negative number/)
            end
          end

          context "when #{option} is of the wrong type" do
            let(:options) { { option => '42' } }

            it 'fails client creation' do
              expect { client }
                .to raise_error(ArgumentError, /#{option} must be a non-negative number/)
            end
          end
        end

        context 'when :connect_timeout is very small' do
          # The driver reads first and checks the deadline second.
          # This means the read (in a monitor) can technically take more than
          # the connect timeout. Restrict to TLS configurations to make
          # the network I/O take longer.
          require_tls

          let(:options) do
            { connect_timeout: 1e-6, server_selection_timeout: 2 }
          end

          it 'allows client creation' do
            expect { client }.not_to raise_error
          end

          context 'non-lb' do
            require_topology :single, :replica_set, :sharded

            it 'fails server selection due to very small timeout' do
              expect { client.database.command(ping: 1) }
                .to raise_error(Mongo::Error::NoServerAvailable)
            end
          end

          context 'lb' do
            require_topology :load_balanced

            it 'fails the operation after successful server selection' do
              expect { client.database.command(ping: 1) }
                .to raise_error(Mongo::Error::SocketTimeoutError, /socket took over.*to connect/)
            end
          end
        end

        context 'when :socket_timeout is very small' do
          # The driver reads first and checks the deadline second.
          # This means the read (in a monitor) can technically take more than
          # the connect timeout. Restrict to TLS configurations to make
          # the network I/O take longer.
          require_tls

          let(:options) do
            { socket_timeout: 1e-6, server_selection_timeout: 2 }
          end

          it 'allows client creation' do
            expect { client }.not_to raise_error
          end

          retry_test
          it 'fails operations due to very small timeout' do
            expect { client.database.command(ping: 1) }
              .to raise_error(Mongo::Error::SocketTimeoutError)
          end
        end
      end

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
          let(:options) { {} }

          it 'sets retry_writes to true' do
            expect(client.options['retry_writes']).to be true
          end
        end
      end

      context 'when compressors are provided' do
        let(:client) do
          new_local_client(
            SpecConfig.instance.addresses,
            SpecConfig.instance.all_test_options.merge(options)
          )
        end

        context 'when the compressor is not supported by the driver' do
          require_warning_clean

          let(:options) do
            { compressors: %w[ snoopy ] }
          end

          it 'does not set the compressor and warns' do
            expect(Mongo::Logger.logger).to receive(:warn).with(/Unsupported compressor/)
            expect(client.options['compressors']).to be_nil
          end

          it 'sets the compression key of the handshake document to an empty array' do
            expect(client.cluster.app_metadata.send(:document)[:compression]).to eq([])
          end

          context 'when one supported compressor and one unsupported compressor are provided' do
            require_compression
            min_server_fcv '3.6'

            let(:options) do
              { compressors: %w[ zlib snoopy ] }
            end

            it 'does not set the unsupported compressor and warns' do
              expect(Mongo::Logger.logger).to receive(:warn).at_least(:once)
              expect(client.options['compressors']).to eq(%w[ zlib ])
            end

            it 'sets the compression key of the handshake document to the list of supported compressors' do
              expect(client.cluster.app_metadata.send(:document)[:compression]).to eq(%w[ zlib ])
            end
          end
        end

        context 'when the compressor is not supported by the server' do
          max_server_version '3.4'

          let(:options) do
            { compressors: %w[ zlib ] }
          end

          it 'does not set the compressor and warns' do
            expect(Mongo::Logger.logger).to receive(:warn).at_least(:once)
            expect(client.cluster.next_primary.monitor.compressor).to be_nil
          end
        end

        context 'when zlib compression is requested' do
          require_zlib_compression

          let(:options) do
            { compressors: %w[ zlib ] }
          end

          it 'sets the compressor' do
            expect(client.options['compressors']).to eq(options[:compressors])
          end

          it 'sends the compressor in the compression key of the handshake document' do
            expect(client.cluster.app_metadata.send(:document)[:compression]).to eq(options[:compressors])
          end

          context 'when server supports compression' do
            min_server_fcv '3.6'

            it 'uses compression for messages' do
              expect(Mongo::Protocol::Compressed).to receive(:new).at_least(:once).and_call_original
              client[TEST_COLL].find({}, limit: 1).first
            end
          end

          it 'does not use compression for authentication messages' do
            expect(Mongo::Protocol::Compressed).not_to receive(:new)
            client.cluster.next_primary.send(:with_connection, &:connect!)
          end
        end

        context 'when snappy compression is requested and supported by the server' do
          min_server_version '3.6'

          let(:options) do
            { compressors: %w[ snappy ] }
          end

          context 'when snappy gem is installed' do
            require_snappy_compression

            it 'creates the client' do
              expect(client.options['compressors']).to eq(%w[ snappy ])
            end
          end

          context 'when snappy gem is not installed' do
            require_no_snappy_compression

            it 'raises an exception' do
              expect do
                client
              end.to raise_error(Mongo::Error::UnmetDependency, /Cannot enable snappy compression/)
            end
          end
        end

        context 'when zstd compression is requested and supported by the server' do
          min_server_version '4.2'

          let(:options) do
            { compressors: %w[ zstd ] }
          end

          context 'when zstd gem is installed' do
            require_zstd_compression

            it 'creates the client' do
              expect(client.options['compressors']).to eq(%w[ zstd ])
            end
          end

          context 'when zstd gem is not installed' do
            require_no_zstd_compression

            it 'raises an exception' do
              expect do
                client
              end.to raise_error(Mongo::Error::UnmetDependency, /Cannot enable zstd compression/)
            end
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
          new_local_client_nmio(
            SpecConfig.instance.addresses,
            SpecConfig.instance.test_options.merge(zlib_compression_level: 1)
          )
        end

        it 'sets the option on the client' do
          expect(client.options[:zlib_compression_level]).to eq(1)
        end
      end

      context 'when ssl options are provided' do
        let(:options) do
          {
            ssl: true,
            ssl_ca_cert: SpecConfig.instance.ca_cert_path,
            ssl_ca_cert_string: 'ca cert string',
            ssl_ca_cert_object: 'ca cert object',
            ssl_cert: SpecConfig.instance.client_cert_path,
            ssl_cert_string: 'cert string',
            ssl_cert_object: 'cert object',
            ssl_key: SpecConfig.instance.client_key_path,
            ssl_key_string: 'key string',
            ssl_key_object: 'key object',
            ssl_key_pass_phrase: 'passphrase',
            ssl_verify: true,
          }
        end

        let(:client) do
          new_local_client_nmio(SINGLE_CLIENT, options)
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
          new_local_client_nmio(SINGLE_CLIENT, read: { mode: :secondary })
        end

        it 'defaults the database to admin' do
          expect(client.database.name).to eq('admin')
        end
      end

      context 'when a database is provided' do
        let(:client) do
          new_local_client_nmio(SINGLE_CLIENT, database: :testdb)
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
          expect(client.cluster.logger).not_to eq(Mongo::Logger.logger)
        end
      end

      context 'when providing a heartbeat_frequency' do
        let(:client) do
          new_local_client_nmio(SINGLE_CLIENT, heartbeat_frequency: 2)
        end

        it 'sets the heartbeat frequency' do
          expect(client.cluster.options[:heartbeat_frequency]).to eq(client.options[:heartbeat_frequency])
        end
      end

      context 'when max_connecting is provided' do
        let(:client) do
          new_local_client_nmio(SINGLE_CLIENT, options)
        end

        context 'when max_connecting is a positive integer' do
          let(:options) do
            { max_connecting: 5 }
          end

          it 'sets the max connecting' do
            expect(client.options[:max_connecting]).to eq(options[:max_connecting])
          end
        end

        context 'when max_connecting is a negative integer' do
          let(:options) do
            { max_connecting: -5 }
          end

          it 'raises an exception' do
            expect { client }.to raise_error(Mongo::Error::InvalidMaxConnecting)
          end
        end
      end

      context 'when min_pool_size is provided' do
        let(:client) { new_local_client_nmio(SINGLE_CLIENT, options) }

        context 'when max_pool_size is provided' do
          context 'when the min_pool_size is greater than the max_pool_size' do
            let(:options) { { min_pool_size: 20, max_pool_size: 10 } }

            it 'raises an Exception' do
              expect { client }
                .to raise_exception(Mongo::Error::InvalidMinPoolSize)
            end
          end

          context 'when the min_pool_size is less than the max_pool_size' do
            let(:options) { { min_pool_size: 10, max_pool_size: 20 } }

            it 'sets the option' do
              expect(client.options[:min_pool_size]).to eq(options[:min_pool_size])
              expect(client.options[:max_pool_size]).to eq(options[:max_pool_size])
            end
          end

          context 'when the min_pool_size is equal to the max_pool_size' do
            let(:options) { { min_pool_size: 10, max_pool_size: 10 } }

            it 'sets the option' do
              expect(client.options[:min_pool_size]).to eq(options[:min_pool_size])
              expect(client.options[:max_pool_size]).to eq(options[:max_pool_size])
            end
          end

          context 'when max_pool_size is zero (unlimited)' do
            let(:options) { { min_pool_size: 10, max_pool_size: 0 } }

            it 'sets the option' do
              expect(client.options[:min_pool_size]).to eq(options[:min_pool_size])
              expect(client.options[:max_pool_size]).to eq(options[:max_pool_size])
            end
          end
        end

        context 'when max_pool_size is not provided' do
          context 'when the min_pool_size is greater than the default max_pool_size' do
            let(:options) { { min_pool_size: 30 } }

            it 'raises an Exception' do
              expect { client }
                .to raise_exception(Mongo::Error::InvalidMinPoolSize)
            end
          end

          context 'when the min_pool_size is less than the default max_pool_size' do
            let(:options) { { min_pool_size: 3 } }

            it 'sets the option' do
              expect(client.options[:min_pool_size]).to eq(options[:min_pool_size])
            end
          end

          context 'when the min_pool_size is equal to the max_pool_size' do
            let(:options) do
              {
                min_pool_size: Mongo::Server::ConnectionPool::DEFAULT_MAX_SIZE
              }
            end

            it 'sets the option' do
              expect(client.options[:min_pool_size]).to eq(options[:min_pool_size])
            end
          end
        end
      end

      context 'when max_pool_size is 0 (unlimited)' do
        let(:client) { new_local_client_nmio(SINGLE_CLIENT, options) }
        let(:options) { { max_pool_size: 0 } }

        it 'sets the option' do
          expect(client.options[:max_pool_size]).to eq(options[:max_pool_size])
        end
      end

      context 'when max_pool_size and min_pool_size are both nil' do
        let(:options) { { min_pool_size: nil, max_pool_size: nil } }
        let(:client) { new_local_client_nmio(SINGLE_CLIENT, options) }

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
          new_local_client_nmio(SINGLE_CLIENT, platform: 'mongoid-6.0.2')
        end

        it 'includes the platform info in the app metadata' do
          expect(app_metadata.client_document[:platform]).to match(/mongoid-6\.0\.2/)
        end
      end

      context 'when platform details are not specified' do
        let(:app_metadata) do
          client.cluster.app_metadata
        end

        let(:client) do
          new_local_client_nmio(SINGLE_CLIENT)
        end

        context 'mri' do
          require_mri

          let(:platform_string) do
            [
              "Ruby #{RUBY_VERSION}",
              RUBY_PLATFORM,
              RbConfig::CONFIG['build'],
              'A',
            ].join(', ')
          end

          it 'does not include the platform info in the app metadata' do
            expect(app_metadata.client_document[:platform]).to eq(platform_string)
          end
        end

        context 'jruby' do
          require_jruby

          let(:platform_string) do
            [
              "JRuby #{JRUBY_VERSION}",
              "like Ruby #{RUBY_VERSION}",
              RUBY_PLATFORM,
              "JVM #{java.lang.System.get_property('java.version')}",
              RbConfig::CONFIG['build'],
              'A',
            ].join(', ')
          end

          it 'does not include the platform info in the app metadata' do
            expect(app_metadata.client_document[:platform]).to eq(platform_string)
          end
        end
      end
    end

    context 'when providing a connection string' do
      context 'when the string uses the SRV Protocol' do
        require_external_connectivity

        let(:uri) { 'mongodb+srv://test5.test.build.10gen.cc/testdb' }
        let(:client) { new_local_client_nmio(uri) }

        it 'sets the database' do
          expect(client.options[:database]).to eq('testdb')
        end
      end

      context 'when a database is provided' do
        let(:uri) { 'mongodb://127.0.0.1:27017/testdb' }
        let(:client) { new_local_client_nmio(uri) }

        it 'sets the database' do
          expect { client[:users] }.not_to raise_error
        end
      end

      context 'when a database is not provided' do
        let(:uri) { 'mongodb://127.0.0.1:27017' }
        let(:client) { new_local_client_nmio(uri) }

        it 'defaults the database to admin' do
          expect(client.database.name).to eq('admin')
        end
      end

      context 'when URI options are provided' do
        let(:uri) { 'mongodb://127.0.0.1:27017/testdb?w=3' }
        let(:client) { new_local_client_nmio(uri) }

        let(:expected_options) do
          Mongo::Options::Redacted.new(
            write_concern: { w: 3 },
            monitoring_io: false,
            database: 'testdb',
            retry_writes: true,
            retry_reads: true
          )
        end

        it 'sets the options' do
          expect(client.options).to eq(expected_options)
        end

        context 'when max_connecting is provided' do
          context 'when max_connecting is a positive integer' do
            let(:uri) do
              'mongodb://127.0.0.1:27017/?maxConnecting=10'
            end

            it 'sets the max connecting' do
              expect(client.options[:max_connecting]).to eq(10)
            end
          end

          context 'when max_connecting is a negative integer' do
            let(:uri) do
              'mongodb://127.0.0.1:27017/?maxConnecting=0'
            end

            it 'raises an exception' do
              expect { client }.to raise_error(Mongo::Error::InvalidMaxConnecting)
            end
          end
        end

        context 'when min_pool_size is provided' do
          context 'when max_pool_size is provided' do
            context 'when the min_pool_size is greater than the max_pool_size' do
              let(:uri) do
                'mongodb://127.0.0.1:27017/?minPoolSize=20&maxPoolSize=10'
              end

              it 'raises an Exception' do
                expect { client }
                  .to raise_exception(Mongo::Error::InvalidMinPoolSize)
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

            context 'when max_pool_size is 0 (unlimited)' do
              let(:uri) do
                'mongodb://127.0.0.1:27017/?minPoolSize=10&maxPoolSize=0'
              end

              it 'sets the option' do
                expect(client.options[:min_pool_size]).to eq(10)
                expect(client.options[:max_pool_size]).to eq(0)
              end
            end
          end

          context 'when max_pool_size is not provided' do
            context 'when the min_pool_size is greater than the default max_pool_size' do
              let(:uri) { 'mongodb://127.0.0.1:27017/?minPoolSize=30' }

              it 'raises an Exception' do
                expect { client }
                  .to raise_exception(Mongo::Error::InvalidMinPoolSize)
              end
            end

            context 'when the min_pool_size is less than the default max_pool_size' do
              let(:uri) { 'mongodb://127.0.0.1:27017/?minPoolSize=3' }

              it 'sets the option' do
                expect(client.options[:min_pool_size]).to eq(3)
              end
            end

            context 'when the min_pool_size is equal to the max_pool_size' do
              let(:uri) { 'mongodb://127.0.0.1:27017/?minPoolSize=5' }

              it 'sets the option' do
                expect(client.options[:min_pool_size]).to eq(5)
              end
            end
          end
        end

        context 'when retryReads URI option is given' do
          context 'it is false' do
            let(:uri) { 'mongodb://127.0.0.1:27017/testdb?retryReads=false' }

            it 'sets the option on the client' do
              expect(client.options[:retry_reads]).to be false
            end
          end

          context 'it is true' do
            let(:uri) { 'mongodb://127.0.0.1:27017/testdb?retryReads=true' }

            it 'sets the option on the client' do
              expect(client.options[:retry_reads]).to be true
            end
          end
        end

        context 'when retryWrites URI option is given' do
          context 'it is false' do
            let(:uri) { 'mongodb://127.0.0.1:27017/testdb?retryWrites=false' }

            it 'sets the option on the client' do
              expect(client.options[:retry_writes]).to be false
            end
          end

          context 'it is true' do
            let(:uri) { 'mongodb://127.0.0.1:27017/testdb?retryWrites=true' }

            it 'sets the option on the client' do
              expect(client.options[:retry_writes]).to be true
            end
          end
        end
      end

      context 'when options are provided not in the string' do
        let(:uri) { 'mongodb://127.0.0.1:27017/testdb' }

        let(:client) do
          new_local_client_nmio(uri, write: { w: 3 })
        end

        let(:expected_options) do
          Mongo::Options::Redacted.new(
            write: { w: 3 },
            monitoring_io: false,
            database: 'testdb',
            retry_writes: true,
            retry_reads: true
          )
        end

        it 'sets the options' do
          expect(client.options).to eq(expected_options)
        end
      end

      context 'when options are provided in the URI and as Ruby options' do
        let(:uri) { 'mongodb://127.0.0.1:27017/testdb?w=3' }

        let(:client) do
          new_local_client_nmio(uri, option_name => { w: 4 })
        end

        let(:expected_options) do
          Mongo::Options::Redacted.new(
            option_name => { w: 4 },
            monitoring_io: false,
            database: 'testdb',
            retry_writes: true,
            retry_reads: true
          )
        end

        shared_examples_for 'allows explicit options to take preference' do
          it 'allows explicit options to take preference' do
            expect(client.options).to eq(expected_options)
          end
        end

        context 'when using :write' do
          let(:option_name) { :write }

          it_behaves_like 'allows explicit options to take preference'
        end

        context 'when using :write_concern' do
          let(:option_name) { :write_concern }

          it_behaves_like 'allows explicit options to take preference'
        end
      end

      context 'when a replica set name is provided' do
        let(:uri) { 'mongodb://127.0.0.1:27017/testdb?replicaSet=testing' }
        let(:client) { new_local_client_nmio(uri) }

        it 'sets the correct cluster topology' do
          expect(client.cluster.topology).to be_a(Mongo::Cluster::Topology::ReplicaSetNoPrimary)
        end
      end
    end

    context 'when Ruby options are provided' do
      let(:client) do
        new_local_client_nmio(SINGLE_CLIENT, options)
      end

      describe 'connection option conflicts' do
        context 'direct_connection: true and multiple seeds' do
          let(:client) do
            new_local_client_nmio([ '127.0.0.1:27017', '127.0.0.2:27017' ], direct_connection: true)
          end

          it 'is rejected' do
            expect { client }
              .to raise_error(ArgumentError, /direct_connection=true cannot be used with multiple seeds/)
          end
        end

        context 'direct_connection: true and connect: :direct' do
          let(:options) do
            { direct_connection: true, connect: :direct }
          end

          it 'is accepted' do
            expect(client.options[:direct_connection]).to be true
            expect(client.options[:connect]).to be :direct
          end
        end

        context 'direct_connection: true and connect: :replica_set' do
          let(:options) do
            { direct_connection: true, connect: :replica_set }
          end

          it 'is rejected' do
            expect { client }
              .to raise_error(
                ArgumentError,
                /Conflicting client options: direct_connection=true and connect=replica_set/
              )
          end
        end

        context 'direct_connection: true and connect: :sharded' do
          let(:options) do
            { direct_connection: true, connect: :sharded }
          end

          it 'is rejected' do
            expect { client }
              .to raise_error(ArgumentError, /Conflicting client options: direct_connection=true and connect=sharded/)
          end
        end

        context 'direct_connection: false and connect: :direct' do
          let(:options) do
            { direct_connection: false, connect: :direct }
          end

          it 'is rejected' do
            expect { client }
              .to raise_error(ArgumentError, /Conflicting client options: direct_connection=false and connect=direct/)
          end
        end

        context 'direct_connection: false and connect: :replica_set' do
          let(:options) do
            { direct_connection: false, connect: :replica_set, replica_set: 'foo' }
          end

          it 'is accepted' do
            expect(client.options[:direct_connection]).to be false
            expect(client.options[:connect]).to be :replica_set
          end
        end

        context 'direct_connection: false and connect: :sharded' do
          let(:options) do
            { direct_connection: false, connect: :sharded }
          end

          it 'is accepted' do
            expect(client.options[:direct_connection]).to be false
            expect(client.options[:connect]).to be :sharded
          end
        end

        context 'load_balanced: true and multiple seeds' do
          let(:client) do
            new_local_client_nmio([ '127.0.0.1:27017', '127.0.0.2:27017' ], load_balanced: true)
          end

          it 'is rejected' do
            expect { client }
              .to raise_error(ArgumentError, /load_balanced=true cannot be used with multiple seeds/)
          end
        end

        context 'load_balanced: false and multiple seeds' do
          let(:client) do
            new_local_client_nmio([ '127.0.0.1:27017', '127.0.0.2:27017' ], load_balanced: false)
          end

          it 'is accepted' do
            expect { client }.not_to raise_error
            expect(client.options[:load_balanced]).to be false
          end
        end

        context 'load_balanced: true and direct_connection: true' do
          let(:client) do
            new_local_client_nmio(SINGLE_CLIENT, load_balanced: true, direct_connection: true)
          end

          it 'is rejected' do
            expect { client }
              .to raise_error(ArgumentError, /direct_connection=true cannot be used with load_balanced=true/)
          end
        end

        context 'load_balanced: true and direct_connection: false' do
          let(:client) do
            new_local_client_nmio(SINGLE_CLIENT, load_balanced: true, direct_connection: false)
          end

          it 'is accepted' do
            expect { client }.not_to raise_error
            expect(client.options[:load_balanced]).to be true
            expect(client.options[:direct_connection]).to be false
          end
        end

        context 'load_balanced: false and direct_connection: true' do
          let(:client) do
            new_local_client_nmio(SINGLE_CLIENT, load_balanced: false, direct_connection: true)
          end

          it 'is accepted' do
            expect { client }.not_to raise_error
            expect(client.options[:load_balanced]).to be false
            expect(client.options[:direct_connection]).to be true
          end
        end

        [ :direct, 'direct', :sharded, 'sharded' ].each do |v|
          context "load_balanced: true and connect: #{v.inspect}" do
            let(:client) do
              new_local_client_nmio(SINGLE_CLIENT, load_balanced: true, connect: v)
            end

            it 'is rejected' do
              expect { client }
                .to raise_error(ArgumentError, /connect=#{v} cannot be used with load_balanced=true/)
            end
          end
        end

        [ nil ].each do |v|
          context "load_balanced: true and connect: #{v.inspect}" do
            let(:client) do
              new_local_client_nmio(SINGLE_CLIENT, load_balanced: true, connect: v)
            end

            it 'is accepted' do
              expect { client }.not_to raise_error
              expect(client.options[:load_balanced]).to be true
              expect(client.options[:connect]).to eq v
            end
          end
        end

        [ :load_balanced, 'load_balanced' ].each do |v|
          context "load_balanced: true and connect: #{v.inspect}" do
            let(:client) do
              new_local_client_nmio(SINGLE_CLIENT, load_balanced: true, connect: v)
            end

            it 'is accepted' do
              expect { client }.not_to raise_error
              expect(client.options[:load_balanced]).to be true
              expect(client.options[:connect]).to eq v
            end
          end

          context "replica_set and connect: #{v.inspect}" do
            let(:client) do
              new_local_client_nmio(SINGLE_CLIENT, replica_set: 'foo', connect: v)
            end

            it 'is rejected' do
              expect { client }
                .to raise_error(ArgumentError, /connect=load_balanced cannot be used with replica_set option/)
            end
          end

          context "direct_connection=true and connect: #{v.inspect}" do
            let(:client) do
              new_local_client_nmio(SINGLE_CLIENT, direct_connection: true, connect: v)
            end

            it 'is rejected' do
              expect { client }
                .to raise_error(
                  ArgumentError,
                  /Conflicting client options: direct_connection=true and connect=load_balanced/
                )
            end
          end

          context "multiple seed addresses and connect: #{v.inspect}" do
            let(:client) do
              new_local_client_nmio([ '127.0.0.1:27017', '127.0.0.1:1234' ], connect: v)
            end

            it 'is rejected' do
              expect { client }
                .to raise_error(ArgumentError, /connect=load_balanced cannot be used with multiple seeds/)
            end
          end
        end

        [ :replica_set, 'replica_set' ].each do |v|
          context "load_balanced: true and connect: #{v.inspect}" do
            let(:client) do
              new_local_client_nmio(SINGLE_CLIENT, load_balanced: true, connect: v, replica_set: 'x')
            end

            it 'is rejected' do
              expect { client }
                .to raise_error(ArgumentError, /connect=replica_set cannot be used with load_balanced=true/)
            end
          end

          context "load_balanced: true and #{v.inspect} option" do
            let(:client) do
              new_local_client_nmio(SINGLE_CLIENT, load_balanced: true, v => 'rs')
            end

            it 'is rejected' do
              expect { client }
                .to raise_error(ArgumentError, /load_balanced=true cannot be used with replica_set option/)
            end
          end
        end

        context 'srv_max_hosts > 0 and load_balanced: true' do
          let(:client) do
            new_local_client_nmio(SINGLE_CLIENT, srv_max_hosts: 1, load_balanced: true)
          end

          it 'it is rejected' do
            expect { client }
              .to raise_error(ArgumentError, /:srv_max_hosts > 0 cannot be used with :load_balanced=true/)
          end
        end

        context 'srv_max_hosts > 0 and replica_set' do
          let(:client) do
            new_local_client_nmio(SINGLE_CLIENT, srv_max_hosts: 1, replica_set: 'rs')
          end

          it 'it is rejected' do
            expect do
              client
            end.to raise_error(ArgumentError, /:srv_max_hosts > 0 cannot be used with :replica_set option/)
          end
        end

        context 'srv_max_hosts < 0' do
          let(:client) do
            new_local_client_nmio(SINGLE_CLIENT, srv_max_hosts: -1)
          end

          it 'is accepted and does not add the srv_max_hosts to uri_options' do
            expect { client }.not_to raise_error
            expect(client.options).not_to have_key(:srv_max_hosts)
          end
        end

        context 'srv_max_hosts invalid type' do
          let(:client) do
            new_local_client_nmio(SINGLE_CLIENT, srv_max_hosts: 'foo')
          end

          it 'is accepted and does not add the srv_max_hosts to uri_options' do
            expect { client }.not_to raise_error
            expect(client.options).not_to have_key(:srv_max_hosts)
          end
        end

        context 'srv_max_hosts with non-SRV URI' do
          let(:client) do
            new_local_client_nmio(SINGLE_CLIENT, srv_max_hosts: 1)
          end

          it 'is rejected' do
            expect { client }
              .to raise_error(ArgumentError, /:srv_max_hosts cannot be used on non-SRV URI/)
          end
        end

        context 'srv_service_name with non-SRV URI' do
          let(:client) do
            new_local_client_nmio(SINGLE_CLIENT, srv_service_name: 'customname')
          end

          it 'is rejected' do
            expect { client }
              .to raise_error(ArgumentError, /:srv_service_name cannot be used on non-SRV URI/)
          end
        end
      end

      context 'with SRV lookups mocked at Resolver' do
        let(:srv_result) do
          double('srv result').tap do |result|
            allow(result).to receive(:empty?).and_return(false)
            allow(result).to receive(:address_strs).and_return(
              [ ClusterConfig.instance.primary_address_str ]
            )
          end
        end

        let(:client) do
          allow_any_instance_of(Mongo::Srv::Resolver).to receive(:get_records).and_return(srv_result)
          allow_any_instance_of(Mongo::Srv::Resolver).to receive(:get_txt_options_string)

          new_local_client_nmio('mongodb+srv://foo.a.b', options)
        end

        context 'when setting srv_max_hosts' do
          let(:srv_max_hosts) { 1 }
          let(:options) { { srv_max_hosts: srv_max_hosts } }

          it 'is accepted and sets srv_max_hosts' do
            expect { client }.not_to raise_error
            expect(client.options[:srv_max_hosts]).to eq(srv_max_hosts)
          end
        end

        context 'when setting srv_max_hosts to 0' do
          let(:srv_max_hosts) { 0 }
          let(:options) { { srv_max_hosts: srv_max_hosts } }

          it 'is accepted sets srv_max_hosts' do
            expect { client }.not_to raise_error
            expect(client.options[:srv_max_hosts]).to eq(srv_max_hosts)
          end
        end

        context 'when setting srv_service_name' do
          let(:srv_service_name) { 'customname' }
          let(:options) { { srv_service_name: srv_service_name } }

          it 'is accepted and sets srv_service_name' do
            expect { client }.not_to raise_error
            expect(client.options[:srv_service_name]).to eq(srv_service_name)
          end
        end
      end

      context ':bg_error_backtrace option' do
        [ true, false, nil, 42 ].each do |valid_value|
          context "valid value: #{valid_value.inspect}" do
            let(:options) do
              { bg_error_backtrace: valid_value }
            end

            it 'is accepted' do
              expect(client.options[:bg_error_backtrace]).to be == valid_value
            end
          end
        end

        context 'invalid value type' do
          let(:options) do
            { bg_error_backtrace: 'yes' }
          end

          it 'is rejected' do
            expect { client }
              .to raise_error(
                ArgumentError,
                /:bg_error_backtrace option value must be true, false, nil or a positive integer/
              )
          end
        end

        context 'invalid value' do
          [ 0, -1, 42.0 ].each do |invalid_value|
            context "invalid value: #{invalid_value.inspect}" do
              let(:options) do
                { bg_error_backtrace: invalid_value }
              end

              it 'is rejected' do
                expect { client }
                  .to raise_error(
                    ArgumentError,
                    /:bg_error_backtrace option value must be true, false, nil or a positive integer/
                  )
              end
            end
          end
        end
      end

      describe ':read option' do
        %i[ primary primary_preferred secondary secondary_preferred nearest ].each do |sym|
          describe sym.to_s do
            context 'when given as symbol' do
              let(:options) do
                { read: { mode: sym } }
              end

              it 'is accepted' do
                # the key got converted to a string here
                expect(client.read_preference).to eq({ 'mode' => sym })
              end
            end

            context 'when given as string' do
              let(:options) do
                { read: { mode: sym.to_s } }
              end

              # string keys are not documented as being allowed
              # but the code accepts them
              it 'is accepted' do
                # the key got converted to a string here
                # the value remains a string
                expect(client.read_preference).to eq({ 'mode' => sym.to_s })
              end
            end
          end
        end

        context 'when not linting' do
          require_no_linting

          it 'rejects bogus read preference as symbol' do
            expect do
              new_local_client_nmio(SINGLE_CLIENT, read: { mode: :bogus })
            end.to raise_error(
              Mongo::Error::InvalidReadOption,
              'Invalid read preference value: {"mode"=>:bogus}: ' \
              'mode bogus is not one of recognized modes'
            )
          end

          it 'rejects bogus read preference as string' do
            expect do
              new_local_client_nmio(SINGLE_CLIENT, read: { mode: 'bogus' })
            end.to raise_error(
              Mongo::Error::InvalidReadOption,
              'Invalid read preference value: {"mode"=>"bogus"}: mode bogus is not one of recognized modes'
            )
          end

          it 'rejects read option specified as a string' do
            expect do
              new_local_client_nmio(SINGLE_CLIENT, read: 'primary')
            end.to raise_error(
              Mongo::Error::InvalidReadOption,
              'Invalid read preference value: "primary": ' \
              'the read preference must be specified as a hash: { mode: "primary" }'
            )
          end

          it 'rejects read option specified as a symbol' do
            expect do
              new_local_client_nmio(SINGLE_CLIENT, read: :primary)
            end.to raise_error(
              Mongo::Error::InvalidReadOption,
              'Invalid read preference value: :primary: ' \
              'the read preference must be specified as a hash: { mode: :primary }'
            )
          end
        end
      end

      context 'when setting read concern options' do
        min_server_fcv '3.2'

        context 'when read concern is valid' do
          let(:options) do
            { read_concern: { level: :local } }
          end

          it 'does not warn' do
            expect(Mongo::Logger.logger).not_to receive(:warn)
            new_local_client_nmio(SpecConfig.instance.addresses, options)
          end
        end

        context 'when read concern has an invalid key' do
          require_no_linting

          let(:options) do
            { read_concern: { hello: :local } }
          end

          it 'logs a warning' do
            expect(Mongo::Logger.logger).to receive(:warn).with(/Read concern has invalid keys: hello/)
            new_local_client_nmio(SpecConfig.instance.addresses, options)
          end
        end

        context 'when read concern has a non-user-settable key' do
          let(:options) do
            { read_concern: { after_cluster_time: 100 } }
          end

          it 'raises an exception' do
            expect do
              new_local_client_nmio(SpecConfig.instance.addresses, options)
            end.to raise_error(
              Mongo::Error::InvalidReadConcern,
              'The after_cluster_time read_concern option cannot be specified by the user'
            )
          end
        end
      end

      context 'when an invalid option is provided' do
        let(:options) do
          { ssl: false, invalid: :test }
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

=begin WriteConcern object support
      context 'when write concern is provided via a WriteConcern object' do
        let(:options) do
          { write_concern: wc }
        end

        let(:wc) { Mongo::WriteConcern.get(w: 2) }

        it 'stores write concern options in client options' do
          expect(client.options[:write_concern]).to eq(
            Mongo::Options::Redacted.new(w: 2))
        end

        it 'caches write concern object' do
          expect(client.write_concern).to be wc
        end
      end
=end

      context ':wrapping_libraries option' do
        let(:options) do
          { wrapping_libraries: wrapping_libraries }
        end

        context 'valid input' do
          context 'symbol keys' do
            let(:wrapping_libraries) do
              [ { name: 'Mongoid', version: '7.1.2' } ].freeze
            end

            it 'works' do
              expect(client.options[:wrapping_libraries]).to be == [ { 'name' => 'Mongoid', 'version' => '7.1.2' } ]
            end
          end

          context 'string keys' do
            let(:wrapping_libraries) do
              [ { 'name' => 'Mongoid', 'version' => '7.1.2' } ].freeze
            end

            it 'works' do
              expect(client.options[:wrapping_libraries]).to be == [ { 'name' => 'Mongoid', 'version' => '7.1.2' } ]
            end
          end

          context 'Redacted keys' do
            let(:wrapping_libraries) do
              [ Mongo::Options::Redacted.new(name: 'Mongoid', version: '7.1.2') ].freeze
            end

            it 'works' do
              expect(client.options[:wrapping_libraries]).to be == [ { 'name' => 'Mongoid', 'version' => '7.1.2' } ]
            end
          end

          context 'two libraries' do
            let(:wrapping_libraries) do
              [
                { name: 'Mongoid', version: '7.1.2' },
                { name: 'Rails', version: '4.0', platform: 'Foobar' },
              ].freeze
            end

            it 'works' do
              expect(client.options[:wrapping_libraries]).to be == [
                { 'name' => 'Mongoid', 'version' => '7.1.2' },
                { 'name' => 'Rails', 'version' => '4.0', 'platform' => 'Foobar' },
              ]
            end
          end

          context 'empty array' do
            let(:wrapping_libraries) do
              []
            end

            it 'works' do
              expect(client.options[:wrapping_libraries]).to be == []
            end
          end

          context 'empty array' do
            let(:wrapping_libraries) do
              nil
            end

            it 'works' do
              expect(client.options[:wrapping_libraries]).to be_nil
            end
          end
        end

        context 'valid input' do
          context 'hash given instead of an array' do
            let(:wrapping_libraries) do
              { name: 'Mongoid', version: '7.1.2' }.freeze
            end

            it 'is rejected' do
              expect { client }
                .to raise_error(ArgumentError, /:wrapping_libraries must be an array of hashes/)
            end
          end

          context 'invalid keys' do
            let(:wrapping_libraries) do
              [ { name: 'Mongoid', invalid: '7.1.2' } ].freeze
            end

            it 'is rejected' do
              expect { client }
                .to raise_error(ArgumentError, /:wrapping_libraries element has invalid keys/)
            end
          end

          context 'value includes |' do
            let(:wrapping_libraries) do
              [ { name: 'Mongoid|on|Rails', version: '7.1.2' } ].freeze
            end

            it 'is rejected' do
              expect { client }
                .to raise_error(ArgumentError, /:wrapping_libraries element value cannot include '|'/)
            end
          end
        end
      end

      context ':auth_mech_properties option' do
        context 'is nil' do
          let(:options) { { auth_mech_properties: nil } }

          it 'creates the client without the option' do
            expect(client.options).not_to have_key(:auth_mech_properties)
          end
        end
      end

      context ':server_api parameter' do
        context 'is a hash with symbol keys' do
          context 'using known keys' do
            let(:options) do
              {
                server_api: {
                  version: '1',
                  strict: true,
                  deprecation_errors: false,
                }
              }
            end

            it 'is accepted' do
              expect(client.options[:server_api]).to be == {
                'version' => '1',
                'strict' => true,
                'deprecation_errors' => false,
              }
            end
          end

          context 'using an unknown version' do
            let(:options) do
              { server_api: { version: '42' } }
            end

            it 'is rejected' do
              expect { client }
                .to raise_error(ArgumentError, 'Unknown server API version: 42')
            end
          end

          context 'using an unknown option' do
            let(:options) do
              { server_api: { vversion: '1' } }
            end

            it 'is rejected' do
              expect { client }
                .to raise_error(ArgumentError, 'Unknown keys under :server_api: "vversion"')
            end
          end

          context 'using a value which is not a hash' do
            let(:options) do
              { server_api: 42 }
            end

            it 'is rejected' do
              expect { client }
                .to raise_error(ArgumentError, ':server_api value must be a hash: 42')
            end
          end
        end

        context 'when connected to a pre-OP_MSG server' do
          max_server_version '3.4'

          let(:options) do
            { server_api: { version: 1 } }
          end

          let(:client) do
            new_local_client(
              SpecConfig.instance.addresses,
              SpecConfig.instance.all_test_options.merge(options)
            )
          end

          it 'constructs the client' do
            expect(client).to be_a(described_class)
          end

          it 'does not discover servers' do
            client.cluster.servers_list.each do |s|
              expect(s.status).to eq('UNKNOWN')
            end
          end

          it 'fails operations' do
            expect { client.command(ping: 1) }
              .to raise_error(Mongo::Error::NoServerAvailable)
          end
        end
      end
    end

    context 'when making a block client' do
      context 'when the block doesn\'t raise an error' do
        let(:block_client) do
          c = nil
          described_class.new(
            SpecConfig.instance.addresses,
            SpecConfig.instance.test_options.merge(database: SpecConfig.instance.test_db)
          ) do |client|
            c = client
          end
          c
        end

        it 'is closed after block' do
          expect(block_client.cluster.connected?).to be false
        end

        context 'with auto encryption options' do
          require_libmongocrypt
          min_server_fcv '4.2'
          require_enterprise
          clean_slate

          include_context 'define shared FLE helpers'
          include_context 'with local kms_providers'

          let(:auto_encryption_options) do
            {
              key_vault_client: key_vault_client,
              key_vault_namespace: key_vault_namespace,
              kms_providers: kms_providers,
              schema_map: schema_map,
              extra_options: extra_options,
            }
          end

          let(:key_vault_client) { new_local_client_nmio(SpecConfig.instance.addresses) }

          let(:block_client) do
            c = nil
            described_class.new(
              SpecConfig.instance.addresses,
              SpecConfig.instance.test_options.merge(
                auto_encryption_options: auto_encryption_options,
                database: SpecConfig.instance.test_db
              )
            ) do |client|
              c = client
            end
            c
          end

          it 'closes all clients after block' do
            expect(block_client.cluster.connected?).to be false
            [
              block_client.encrypter.mongocryptd_client,
              block_client.encrypter.key_vault_client,
              block_client.encrypter.metadata_client
            ].each do |crypt_client|
              expect(crypt_client.cluster.connected?).to be false
            end
          end
        end
      end

      context 'when the block raises an error' do
        it 'is closed after the block' do
          block_client_raise = nil
          expect do
            described_class.new(
              SpecConfig.instance.addresses,
              SpecConfig.instance.test_options.merge(database: SpecConfig.instance.test_db)
            ) do |client|
              block_client_raise = client
              raise 'This is an error!'
            end
          end.to raise_error(StandardError, 'This is an error!')
          expect(block_client_raise.cluster.connected?).to be false
        end
      end

      context 'when the hosts given include the protocol' do
        it 'raises an error on mongodb://' do
          expect do
            described_class.new([ 'mongodb://127.0.0.1:27017/test' ])
          end.to raise_error(ArgumentError,
                             "Host 'mongodb://127.0.0.1:27017/test' should not contain protocol. " \
                             'Did you mean to not use an array?')
        end

        it 'raises an error on mongodb+srv://' do
          expect do
            described_class.new([ 'mongodb+srv://127.0.0.1:27017/test' ])
          end.to raise_error(ArgumentError,
                             "Host 'mongodb+srv://127.0.0.1:27017/test' should not contain protocol. " \
                             'Did you mean to not use an array?')
        end

        it 'raises an error on multiple items' do
          expect do
            described_class.new([ '127.0.0.1:27017', 'mongodb+srv://127.0.0.1:27017/test' ])
          end.to raise_error(ArgumentError,
                             "Host 'mongodb+srv://127.0.0.1:27017/test' should not contain protocol. " \
                             'Did you mean to not use an array?')
        end

        it 'raises an error only at beginning of string' do
          expect do
            described_class
              .new([ 'somethingmongodb://127.0.0.1:27017/test', 'mongodb+srv://127.0.0.1:27017/test' ])
          end.to raise_error(ArgumentError,
                             "Host 'mongodb+srv://127.0.0.1:27017/test' should not contain protocol. " \
                             'Did you mean to not use an array?')
        end

        it 'raises an error with different case' do
          expect { described_class.new([ 'MongOdB://127.0.0.1:27017/test' ]) }
            .to raise_error(ArgumentError,
                            "Host 'MongOdB://127.0.0.1:27017/test' should not contain protocol. " \
                            'Did you mean to not use an array?')
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
      client.subscribe(Mongo::Monitoring::SERVER_HEARTBEAT, subscriber)
      expect(monitoring.present_subscribers.length).to eq(1)
      expect(monitoring.subscribers[Mongo::Monitoring::SERVER_HEARTBEAT].length).to eq(1)

      # this duplicates the client
      expect(new_monitoring.present_subscribers.length).to eq(1)
      expect(new_monitoring.subscribers[Mongo::Monitoring::SERVER_HEARTBEAT].length).to eq(1)
    end

    it 'does not change subscribers on original client' do
      monitoring.subscribers.clear
      client.subscribe(Mongo::Monitoring::SERVER_HEARTBEAT, subscriber)
      expect(monitoring.present_subscribers.length).to eq(1)
      expect(monitoring.subscribers[Mongo::Monitoring::SERVER_HEARTBEAT].length).to eq(1)

      new_client.subscribe(Mongo::Monitoring::SERVER_HEARTBEAT, subscriber)
      new_client.subscribe(Mongo::Monitoring::SERVER_HEARTBEAT, subscriber)
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
      client.subscribe(Mongo::Monitoring::SERVER_HEARTBEAT, subscriber)
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
      client.subscribe(Mongo::Monitoring::SERVER_HEARTBEAT, subscriber)
      expect(monitoring.present_subscribers.length).to eq(1)
      expect(monitoring.subscribers[Mongo::Monitoring::SERVER_HEARTBEAT].length).to eq(1)

      new_client.subscribe(Mongo::Monitoring::SERVER_HEARTBEAT, subscriber)
      new_client.subscribe(Mongo::Monitoring::SERVER_HEARTBEAT, subscriber)
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
      new_local_client_nmio(SINGLE_CLIENT, database: SpecConfig.instance.test_db)
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
        expect { client.use(nil) }
          .to raise_error(Mongo::Error::InvalidDatabaseName)
      end
    end
  end

  describe '#with' do
    let(:client) do
      new_local_client_nmio(SINGLE_CLIENT, database: SpecConfig.instance.test_db)
    end

    context 'when providing nil' do
      it 'returns the cloned client' do
        expect(client.with(nil)).to eq(client)
      end
    end

    context 'when the app_name is changed' do
      let(:client) { authorized_client }
      let(:original_options) { client.options }
      let(:new_options) { { app_name: 'client_test' } }
      let(:new_client) { authorized_client.with(new_options) }

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

    context 'when direct_connection option is given' do
      let(:client) do
        options = SpecConfig.instance.test_options
        options.delete(:connect)
        new_local_client(SpecConfig.instance.addresses, options)
      end

      let(:new_client) do
        client.with(new_options)
      end

      before do
        expect(client.options[:direct_connection]).to be_nil
      end

      context 'direct_connection set to false' do
        let(:new_options) do
          { direct_connection: false }
        end

        it 'is accepted' do
          expect(new_client.options[:direct_connection]).to be false
        end
      end

      context 'direct_connection set to true' do
        let(:new_options) do
          { direct_connection: true }
        end

        context 'in single topology' do
          require_topology :single

          it 'is accepted' do
            expect(new_client.options[:direct_connection]).to be true
            expect(new_client.cluster.topology).to be_a(Mongo::Cluster::Topology::Single)
          end
        end

        context 'in replica set or sharded cluster topology' do
          require_topology :replica_set, :sharded

          it 'is rejected' do
            expect { new_client }
              .to raise_error(ArgumentError, /direct_connection=true cannot be used with topologies other than Single/)
          end

          context 'when a new cluster is created' do
            let(:new_options) do
              { direct_connection: true, app_name: 'new-client' }
            end

            it 'is rejected' do
              expect { new_client }
                .to raise_error(ArgumentError,
                                /direct_connection=true cannot be used with topologies other than Single/)
            end
          end
        end
      end
    end

    context 'when the write concern is not changed' do
      let(:client) do
        new_local_client_nmio(
          SINGLE_CLIENT,
          read: { mode: :secondary },
          write: { w: 1 },
          database: SpecConfig.instance.test_db
        )
      end

      let(:new_client) { client.with(read: { mode: :primary }) }

      let(:new_options) do
        Mongo::Options::Redacted.new(
          read: { mode: :primary },
          write: { w: 1 },
          monitoring_io: false,
          database: SpecConfig.instance.test_db,
          retry_writes: true,
          retry_reads: true
        )
      end

      let(:original_options) do
        Mongo::Options::Redacted.new(
          read: { mode: :secondary },
          write: { w: 1 },
          monitoring_io: false,
          database: SpecConfig.instance.test_db,
          retry_writes: true,
          retry_reads: true
        )
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
        new_local_client(
          SINGLE_CLIENT,
          { monitoring_io: false }.merge(client_options)
        )
      end

      let(:client_options) do
        { write: { w: 1 } }
      end

      context 'when the write concern has not been accessed' do
        let(:new_client) { client.with(write: { w: 0 }) }

        let(:get_last_error) do
          new_client.write_concern.get_last_error
        end

        it 'returns the correct write concern' do
          expect(get_last_error).to be_nil
        end
      end

      context 'when the write concern has been accessed' do
        let(:new_client) do
          client.write_concern
          client.with(write: { w: 0 })
        end

        let(:get_last_error) do
          new_client.write_concern.get_last_error
        end

        it 'returns the correct write concern' do
          expect(get_last_error).to be_nil
        end
      end

      context 'when write concern is given as :write' do
        let(:client_options) do
          { write: { w: 1 } }
        end

        it 'sets :write option' do
          expect(client.options[:write]).to eq(Mongo::Options::Redacted.new(w: 1))
        end

        it 'does not set :write_concern option' do
          expect(client.options[:write_concern]).to be_nil
        end

        it 'returns correct write concern' do
          expect(client.write_concern).to be_a(Mongo::WriteConcern::Acknowledged)
          expect(client.write_concern.options).to eq(w: 1)
        end
      end

      context 'when write concern is given as :write_concern' do
        let(:client_options) do
          { write_concern: { w: 1 } }
        end

        it 'sets :write_concern option' do
          expect(client.options[:write_concern]).to eq(Mongo::Options::Redacted.new(w: 1))
        end

        it 'does not set :write option' do
          expect(client.options[:write]).to be_nil
        end

        it 'returns correct write concern' do
          expect(client.write_concern).to be_a(Mongo::WriteConcern::Acknowledged)
          expect(client.write_concern.options).to eq(w: 1)
        end
      end

      context 'when write concern is given as both :write and :write_concern' do
        context 'with identical values' do
          let(:client_options) do
            { write: { w: 1 }, write_concern: { w: 1 } }
          end

          it 'sets :write_concern option' do
            expect(client.options[:write_concern]).to eq(Mongo::Options::Redacted.new(w: 1))
          end

          it 'sets :write option' do
            expect(client.options[:write]).to eq(Mongo::Options::Redacted.new(w: 1))
          end

          it 'returns correct write concern' do
            expect(client.write_concern).to be_a(Mongo::WriteConcern::Acknowledged)
            expect(client.write_concern.options).to eq(w: 1)
          end
        end

        context 'with different values' do
          let(:client_options) do
            { write: { w: 1 }, write_concern: { w: 2 } }
          end

          it 'raises an exception' do
            expect do
              client
            end.to raise_error(ArgumentError, /If :write and :write_concern are both given, they must be identical/)
          end
        end
      end

      context 'when #with uses a different write concern option name' do
        context 'from :write_concern to :write' do
          let(:client_options) do
            { write_concern: { w: 1 } }
          end

          let(:new_client) { client.with(write: { w: 2 }) }

          it 'uses the new option' do
            expect(new_client.options[:write]).to eq(Mongo::Options::Redacted.new(w: 2))
            expect(new_client.options[:write_concern]).to be_nil
          end
        end

        context 'from :write to :write_concern' do
          let(:client_options) do
            { write: { w: 1 } }
          end

          let(:new_client) { client.with(write_concern: { w: 2 }) }

          it 'uses the new option' do
            expect(new_client.options[:write_concern]).to eq(Mongo::Options::Redacted.new(w: 2))
            expect(new_client.options[:write]).to be_nil
          end
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
        new_local_client_nmio([ '[::1]:27017' ], database: SpecConfig.instance.test_db)
      end

      context 'when providing nil' do
        it 'returns the cloned client' do
          expect(client.with(nil)).to eq(client)
        end
      end

      context 'when changing options' do
        let(:new_options) { { app_name: 'client_test' } }
        let(:new_client) { client.with(new_options) }

        it 'returns a new client' do
          expect(new_client).not_to equal(client)
        end
      end
    end

    context 'when new client has a new cluster' do
      let(:client) do
        new_local_client(
          SINGLE_CLIENT,
          database: SpecConfig.instance.test_db,
          server_selection_timeout: 0.5,
          socket_timeout: 0.1, connect_timeout: 0.1, populator_io: false
        )
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
      let(:sdam_proc) do
        proc do |client|
          client.subscribe(Mongo::Monitoring::SERVER_HEARTBEAT, subscriber)
        end
      end

      let(:client) do
        new_local_client(
          SpecConfig.instance.addresses,
          SpecConfig.instance.test_options.merge(
            sdam_proc: sdam_proc,
            connect_timeout: 3.08, socket_timeout: 3.09,
            server_selection_timeout: 2.92,
            heartbeat_frequency: 100,
            database: SpecConfig.instance.test_db
          )
        )
      end

      let(:new_client) do
        client.with(app_name: 'foo').tap do |new_client|
          expect(new_client.cluster).not_to be == client.cluster
        end
      end

      before do
        client.cluster.next_primary
        events = subscriber.select_started_events(Mongo::Monitoring::Event::ServerHeartbeatStarted)
        if ClusterConfig.instance.topology == :load_balanced
          # No server monitoring in LB topology
          expect(events.length).to be == 0
        else
          expect(events.length).to be > 0
        end
      end

      it 'does not copy sdam_proc option to new client' do
        expect(new_client.options[:sdam_proc]).to be_nil
      end

      it 'does not notify subscribers set up by sdam_proc' do
        # On 4.4, the push monitor also is receiving heartbeats.
        # Give those some time to be processed.
        sleep 2

        if ClusterConfig.instance.topology == :load_balanced
          # No server monitoring in LB topology
          expect(subscriber.started_events.length).to eq 0
        else
          expect(subscriber.started_events.length).to be > 0
        end
        subscriber.started_events.clear

        # If this test takes longer than heartbeat interval,
        # subscriber may receive events from the original client.

        new_client.cluster.next_primary

        # Diagnostics
        # rubocop:disable Style/IfUnlessModifier, Lint/Debugger
        unless subscriber.started_events.empty?
          p subscriber.started_events
        end
        # rubocop:enable Style/IfUnlessModifier, Lint/Debugger

        expect(subscriber.started_events.length).to eq 0
        expect(new_client.cluster.topology.class).not_to be Mongo::Cluster::Topology::Unknown
      end
    end

    context 'when :server_api is changed' do
      let(:client) do
        new_local_client_nmio(SINGLE_CLIENT)
      end

      let(:new_client) do
        client.with(server_api: { version: '1' })
      end

      it 'changes :server_api' do
        expect(new_client.options[:server_api]).to be == { 'version' => '1' }
      end
    end

    context 'when :server_api is cleared' do
      let(:client) do
        new_local_client_nmio(SINGLE_CLIENT, server_api: { version: '1' })
      end

      let(:new_client) do
        client.with(server_api: nil)
      end

      it 'clears :server_api' do
        expect(new_client.options[:server_api]).to be_nil
      end
    end
  end

  describe '#dup' do
    let(:client) do
      new_local_client_nmio(
        SINGLE_CLIENT,
        read: { mode: :primary },
        database: SpecConfig.instance.test_db
      )
    end

    let(:new_client) { client.dup }

    it 'creates a client with Redacted options' do
      expect(new_client.options).to be_a(Mongo::Options::Redacted)
    end

    it_behaves_like 'duplicated client with reused monitoring'
  end
end
# rubocop:enable RSpec/ExpectInHook, RSpec/MessageSpies, RSpec/ExampleLength
# rubocop:enable RSpec/ContextWording, RSpec/RepeatedExampleGroupDescription
# rubocop:enable RSpec/ExampleWording, Style/BlockComments, RSpec/AnyInstance
# rubocop:enable RSpec/VerifiedDoubles
