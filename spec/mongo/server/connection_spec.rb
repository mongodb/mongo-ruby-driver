# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

# fails intermittently in evergreen
describe Mongo::Server::Connection do
  class ConnectionSpecTestException < Exception; end

  clean_slate_for_all

  let(:generation_manager) do
    Mongo::Server::ConnectionPool::GenerationManager.new(server: server)
  end

  let!(:address) do
    default_address
  end

  let(:monitoring) do
    Mongo::Monitoring.new(monitoring: false)
  end

  let(:listeners) do
    Mongo::Event::Listeners.new
  end

  let(:app_metadata) do
    Mongo::Server::AppMetadata.new(authorized_client.cluster.options)
  end

  let(:cluster) do
    double('cluster').tap do |cl|
      allow(cl).to receive(:topology).and_return(topology)
      allow(cl).to receive(:app_metadata).and_return(app_metadata)
      allow(cl).to receive(:options).and_return({})
      allow(cl).to receive(:cluster_time).and_return(nil)
      allow(cl).to receive(:update_cluster_time)
      allow(cl).to receive(:run_sdam_flow)
    end
  end

  declare_topology_double

  let(:server_options) { SpecConfig.instance.test_options.merge(monitoring_io: false) }
  let(:server) do
    register_server(
      Mongo::Server.new(address, cluster, monitoring, listeners, server_options.merge(
        # Normally the load_balancer option is set by the cluster
        load_balancer: ClusterConfig.instance.topology == :load_balanced,
      ))
    )
  end

  let(:monitored_server) do
    register_server(
      Mongo::Server.new(address, cluster, monitoring, listeners,
        SpecConfig.instance.test_options.merge(monitoring_io: false)
      ).tap do |server|
        allow(server).to receive(:description).and_return(ClusterConfig.instance.primary_description)
        expect(server).not_to be_unknown
      end
    )
  end

  let(:pool) do
    double('pool').tap do |pool|
      allow(pool).to receive(:close)
      allow(pool).to receive(:generation_manager).and_return(generation_manager)
    end
  end

  describe '#connect!' do

    shared_examples_for 'keeps server type and topology' do
      it 'does not mark server unknown' do
        expect(server).not_to receive(:unknown!)
        error
      end
    end

    shared_examples_for 'marks server unknown' do
      it 'marks server unknown' do
        expect(server).to receive(:unknown!)
        error
      end
    end

    context 'when no socket exists' do

      let(:connection) do
        described_class.new(server, server.options.merge(connection_pool: pool))
      end

      let(:result) do
        connection.connect!
      end

      let(:socket) do
        connection.send(:socket)
      end

      it 'returns true' do
        expect(result).to be true
      end

      it 'creates a socket' do
        result
        expect(socket).to_not be_nil
      end

      it 'connects the socket' do
        result
        expect(socket).to be_alive
      end

      shared_examples_for 'failing connection' do
        it 'raises an exception' do
          expect(error).to be_a(Exception)
        end

        it 'clears socket' do
          error
          expect(connection.send(:socket)).to be nil
        end

        context 'when connection fails' do
          let(:description) do
            double('description').tap do |description|
              allow(description).to receive(:arbiter?).and_return(false)
            end
          end

          let(:first_pending_connection) do
            double('pending connection 1').tap do |conn|
              conn.should receive(:handshake_and_authenticate!).and_raise(exception)
            end
          end

          let(:second_pending_connection) do
            double('pending connection 2').tap do |conn|
              conn.should receive(:handshake_and_authenticate!).and_raise(ConnectionSpecTestException)
            end
          end

          it 'attempts to reconnect if asked to connect again' do
            RSpec::Mocks.with_temporary_scope do
              Mongo::Server::PendingConnection.should receive(:new).ordered.and_return(first_pending_connection)
              Mongo::Server::PendingConnection.should receive(:new).ordered.and_return(second_pending_connection)

              expect do
                connection.connect!
              end.to raise_error(exception)

              expect do
                connection.connect!
              end.to raise_error(ConnectionSpecTestException)
            end
          end
        end
      end

      shared_examples_for 'failing connection with server diagnostics' do
        it_behaves_like 'failing connection'

        it 'adds server diagnostics' do
          error.message.should =~ /on #{connection.address}/
        end
      end

      shared_examples_for 'logs a warning' do
        require_warning_clean

        it 'logs a warning' do
          messages = []
          expect(Mongo::Logger.logger).to receive(:warn) do |msg|
            messages << msg
          end

          expect(error).not_to be nil

          messages.any? { |msg| msg.include?(expected_message) }.should be true
        end

      end

      shared_examples_for 'adds server diagnostics' do
        require_warning_clean

        it 'adds server diagnostics' do
          messages = []
          expect(Mongo::Logger.logger).to receive(:warn) do |msg|
            messages << msg
          end

          expect(error).not_to be nil

          messages.any? { |msg| msg =~ /on #{connection.address}/ }.should be true
        end

      end

      context 'when #handshake! dependency raises a non-network exception' do

        let(:exception) do
          Mongo::Error::OperationFailure.new
        end

        let(:error) do
          # The exception is mutated when notes are added to it
          expect_any_instance_of(Mongo::Socket).to receive(:write).and_raise(exception.dup)
          begin
            connection.connect!
          rescue Exception => e
            e
          else
            nil
          end
        end

        let(:expected_message) do
          "MONGODB | Failed to handshake with #{address}: #{error.class}: #{error}"
        end

        # The server diagnostics only apply to network exceptions.
        # If non-network exceptions can be legitimately raised during
        # handshake, and it makes sense to indicate which server the
        # corresponding request was sent to, we should apply server
        # diagnostics to non-network errors also.
        it_behaves_like 'failing connection'
        it_behaves_like 'keeps server type and topology'
        it_behaves_like 'logs a warning'
      end

      context 'when #handshake! dependency raises a network exception' do
        let(:exception) do
          Mongo::Error::SocketError.new.tap do |exc|
            allow(exc).to receive(:service_id).and_return('fake')
          end
        end

        let(:error) do
          # The exception is mutated when notes are added to it
          expect_any_instance_of(Mongo::Socket).to receive(:write).and_raise(exception)
          allow(connection).to receive(:service_id).and_return('fake')
          begin
            connection.connect!
          rescue Exception => e
            e
          else
            nil
          end
        end

        let(:expected_message) do
          "MONGODB | Failed to handshake with #{address}: #{error.class}: #{error}"
        end

        it_behaves_like 'failing connection with server diagnostics'
        it_behaves_like 'marks server unknown'
        it_behaves_like 'logs a warning'
        it_behaves_like 'adds server diagnostics'
      end

      context 'when #authenticate! raises an exception' do
        require_auth

        let(:server_options) do
          Mongo::Client.canonicalize_ruby_options(
            SpecConfig.instance.all_test_options,
          ).update(monitoring_io: false)
        end

        let(:exception) do
          Mongo::Error::OperationFailure.new
        end

        let(:error) do
          # Speculative auth - would be reported as handshake failure
          expect(Mongo::Auth).to receive(:get).ordered.and_call_original
          # The actual authentication call
          expect(Mongo::Auth).to receive(:get).ordered.and_raise(exception)
          expect(connection.send(:socket)).to be nil
          begin
            connection.connect!
          rescue Exception => e
            e
          else
            nil
          end
        end

        let(:expected_message) do
          "MONGODB | Failed to authenticate to #{address}: #{error.class}: #{error}"
        end

        it_behaves_like 'failing connection'
        it_behaves_like 'logs a warning'
      end

      context 'when a non-Mongo exception is raised' do
        let(:exception) do
          SystemExit.new
        end

        let(:error) do
          expect_any_instance_of(Mongo::Server::PendingConnection).to receive(:authenticate!).and_raise(exception)
          begin
            connection.connect!
          rescue Exception => e
            e
          else
            nil
          end
        end

        it_behaves_like 'failing connection'
      end
    end

    context 'when a socket exists' do

      let(:connection) do
        described_class.new(server, server.options.merge(connection_pool: pool))
      end

      let(:socket) do
        connection.send(:socket)
      end

      it 'keeps the socket alive' do
        expect(connection.connect!).to be true
        expect(connection.connect!).to be true
        expect(socket).to be_alive
      end

      it 'retains socket object' do
        expect(connection.connect!).to be true
        socket_id = connection.send(:socket).object_id
        expect(connection.connect!).to be true
        new_socket_id = connection.send(:socket).object_id
        expect(new_socket_id).to eq(socket_id)
      end
    end

=begin These assertions require a working cluster with working SDAM flow, which the tests do not configure
    shared_examples_for 'does not disconnect connection pool' do
      it 'does not disconnect non-monitoring sockets' do
        allow(server).to receive(:pool).and_return(pool)
        expect(pool).not_to receive(:disconnect!)
        error
      end
    end

    shared_examples_for 'disconnects connection pool' do
      it 'disconnects non-monitoring sockets' do
        expect(server).to receive(:pool).at_least(:once).and_return(pool)
        expect(pool).to receive(:disconnect!).and_return(true)
        error
      end
    end
=end

    let(:auth_mechanism) do
      if ClusterConfig.instance.server_version >= '3'
        Mongo::Auth::Scram
      else
        Mongo::Auth::CR
      end
    end

    context 'when user credentials exist' do
      require_no_external_user

      let(:server) { monitored_server }

      context 'when the user is not authorized' do

        let(:connection) do
          described_class.new(
            server,
            SpecConfig.instance.test_options.merge(
              user: 'notauser',
              password: 'password',
              database: SpecConfig.instance.test_db,
              heartbeat_frequency: 30,
              connection_pool: pool,
            )
          )
        end

        let(:error) do
          begin
            connection.send(:connect!)
          rescue => ex
            ex
          else
            nil
          end
        end

        context 'not checking pool disconnection' do
          before do
            allow(cluster).to receive(:pool).with(server).and_return(pool)
            allow(pool).to receive(:disconnect!).and_return(true)
          end

          it 'raises an error' do
            expect(error).to be_a(Mongo::Auth::Unauthorized)
          end

          #it_behaves_like 'disconnects connection pool'
          it_behaves_like 'marks server unknown'
        end

        # need a separate context here, otherwise disconnect expectation
        # is ignored due to allowing disconnects in the other context
        context 'checking pool disconnection' do
          #it_behaves_like 'disconnects connection pool'
        end
      end

      context 'socket timeout during auth' do
        let(:connection) do
          described_class.new(
            server,
            SpecConfig.instance.test_options.merge(
              :user => SpecConfig.instance.test_user.name,
              :password => SpecConfig.instance.test_user.password,
              :database => SpecConfig.instance.test_user.database )
          )
        end

        let(:error) do
          expect_any_instance_of(auth_mechanism).to receive(:login).and_raise(Mongo::Error::SocketTimeoutError)
          begin
            connection.send(:connect!)
          rescue => ex
            ex
          else
            nil
          end
        end

        it 'propagates the error' do
          expect(error).to be_a(Mongo::Error::SocketTimeoutError)
        end

        #it_behaves_like 'does not disconnect connection pool'
        it_behaves_like 'keeps server type and topology'
      end

      context 'non-timeout socket exception during auth' do
        let(:connection) do
          described_class.new(
            server,
            SpecConfig.instance.test_options.merge(
              :user => SpecConfig.instance.test_user.name,
              :password => SpecConfig.instance.test_user.password,
              :database => SpecConfig.instance.test_user.database )
          )
        end

        let(:exception) do
          Mongo::Error::SocketError.new.tap do |exc|
            if server.load_balancer?
              allow(exc).to receive(:service_id).and_return('fake')
            end
          end
        end

        let(:error) do
          expect_any_instance_of(auth_mechanism).to receive(:login).and_raise(exception)
          begin
            connection.send(:connect!)
          rescue => ex
            ex
          else
            nil
          end
        end

        it 'propagates the error' do
          expect(error).to be_a(Mongo::Error::SocketError)
        end

        #it_behaves_like 'disconnects connection pool'
        it_behaves_like 'marks server unknown'
      end

      describe 'when the user is authorized' do

        let(:connection) do
          described_class.new(
            server,
            SpecConfig.instance.test_options.merge(
              user: SpecConfig.instance.test_user.name,
              password: SpecConfig.instance.test_user.password,
              database: SpecConfig.instance.test_user.database,
              connection_pool: pool,
            )
          )
        end

        before do
          connection.connect!
        end

        it 'sets the connection as connected' do
          expect(connection).to be_connected
        end
      end
    end

    context 'connecting to arbiter' do
      require_topology :replica_set

      before(:all) do
        unless ENV['HAVE_ARBITER']
          skip 'Test requires an arbiter in the deployment'
        end
      end

      let(:arbiter_server) do
        authorized_client.cluster.servers_list.each do |server|
          server.scan!
        end
        server = authorized_client.cluster.servers_list.detect do |server|
          server.arbiter?
        end.tap do |server|
          raise 'No arbiter in the deployment' unless server
        end
      end

      shared_examples_for 'does not authenticate' do
        let(:client) do
          new_local_client([address],
            SpecConfig.instance.test_options.merge(
              :user => 'bogus',
              :password => 'bogus',
              :database => 'bogus'
            ).merge(connect: :direct),
          )
        end

        let(:connection) do
          described_class.new(
            server,
          )
        end

        let(:ping) do
          client.database.command(ping: 1)
        end

        it 'does not authenticate' do
          ClientRegistry.instance.close_all_clients

          expect_any_instance_of(Mongo::Server::Connection).not_to receive(:authenticate!)

          expect(ping.documents.first['ok']).to eq(1) rescue nil
        end
      end

      context 'without me mismatch' do
        let(:address) do
          arbiter_server.address.to_s
        end

        it_behaves_like 'does not authenticate'
      end

      context 'with me mismatch' do
        let(:address) do
          "#{ClusterConfig.instance.alternate_address.host}:#{arbiter_server.address.port}"
        end

        it_behaves_like 'does not authenticate'
      end
    end

  end

  describe '#disconnect!' do

    context 'when a socket is not connected' do

      let(:connection) do
        described_class.new(server, server.options.merge(connection_pool: pool))
      end

      it 'does not raise an error' do
        expect(connection.disconnect!).to be true
      end
    end

    context 'when a socket is connected' do

      let(:connection) do
        described_class.new(server, server.options.merge(connection_pool: pool))
      end

      before do
        connection.connect!
        connection.disconnect!
      end

      it 'disconnects the socket' do
        expect(connection.send(:socket)).to be_nil
      end
    end
  end

  describe '#dispatch' do
    require_no_required_api_version

    let(:server) { monitored_server }

    let(:context) { Mongo::Operation::Context.new }

    let!(:connection) do
      described_class.new(
        server,
        SpecConfig.instance.test_options.merge(
          database: SpecConfig.instance.test_user.database,
          connection_pool: pool,
        ).merge(Mongo::Utils.shallow_symbolize_keys(Mongo::Client.canonicalize_ruby_options(
          SpecConfig.instance.credentials_or_external_user(
            user: SpecConfig.instance.test_user.name,
            password: SpecConfig.instance.test_user.password,
          ),
        )))
      ).tap do |connection|
        connection.connect!
      end
    end

    (0..2).each do |i|
      let("msg#{i}".to_sym) do
        Mongo::Protocol::Msg.new(
          [],
          {},
          {ping: 1, :$db => SpecConfig.instance.test_db}
        )
      end
    end

    context 'when providing a single message' do

      let(:reply) do
        connection.dispatch([ msg0 ], context)
      end

      it 'it dispatches the message to the socket' do
        expect(reply.payload['reply']['ok']).to eq(1.0)
      end
    end

    context 'when providing multiple messages' do

      let(:reply) do
        connection.dispatch([ msg0, msg1 ], context)
      end

      it 'raises ArgumentError' do
        expect do
          reply
        end.to raise_error(ArgumentError, 'Can only dispatch one message at a time')
      end
    end

    context 'when the response_to does not match the request_id' do

      before do
        connection.dispatch([ msg0 ], context)
        # Fake a query for which we did not read the response. See RUBY-1117
        allow(msg1).to receive(:replyable?) { false }
        connection.dispatch([ msg1 ], context)
      end

      it 'raises an UnexpectedResponse error' do
        expect {
          connection.dispatch([ msg0 ], context)
        }.to raise_error(Mongo::Error::UnexpectedResponse,
          /Got response for request ID \d+ but expected response for request ID \d+/)
      end

      it 'marks connection perished' do
        expect {
          connection.dispatch([ msg0 ], context)
        }.to raise_error(Mongo::Error::UnexpectedResponse)

        connection.should be_error
      end

      it 'makes the connection no longer usable' do
        expect {
          connection.dispatch([ msg0 ], context)
        }.to raise_error(Mongo::Error::UnexpectedResponse)

        expect {
          connection.dispatch([ msg0 ], context)
        }.to raise_error(Mongo::Error::ConnectionPerished)
      end
    end

    context 'when a request is interrupted (Thread.kill)' do
      require_no_required_api_version

      before do
        authorized_collection.delete_many
        connection.dispatch([ msg0 ], context)
      end

      it 'closes the socket and does not use it for subsequent requests' do
        t = Thread.new {
          # Kill the thread just before the reply is read
          allow(Mongo::Protocol::Reply).to receive(:deserialize_header) { t.kill && !t.alive? }
          connection.dispatch([ msg1 ], context)
        }
        t.join
        allow(Mongo::Protocol::Message).to receive(:deserialize_header).and_call_original
        resp = connection.dispatch([ msg2 ], context)
        expect(resp.payload['reply']['ok']).to eq(1.0)
      end
    end

    context 'when the message exceeds the max size' do
      require_no_linting

      let(:command) do
        Mongo::Protocol::Msg.new(
          [],
          {},
          {ping: 1, padding: 'x'*16384, :$db => SpecConfig.instance.test_db}
        )
      end

      let(:reply) do
        connection.dispatch([ command ], context)
      end

      it 'checks the size against the max bson size' do
        # 100 works for non-x509 auth.
        # 10 is needed for x509 auth due to smaller payloads, apparently.
        expect_any_instance_of(Mongo::Server::Description).to receive(
          :max_bson_object_size).at_least(:once).and_return(10)
        expect do
          reply
        end.to raise_exception(Mongo::Error::MaxBSONSize)
      end
    end

    context 'when a network error occurs' do
      let(:server) do
        authorized_client.cluster.next_primary.tap do |server|
          # to ensure the server stays in unknown state for the duration
          # of the test, i.e. to avoid racing with the monitor thread
          # which may put the server back into non-unknown state before
          # we can verify that the server was marked unknown, kill off
          # the monitor thread.
          unless ClusterConfig.instance.topology == :load_balanced
            server.monitor.instance_variable_get('@thread').kill
          end
        end
      end

      let(:socket) do
        connection.connect!
        connection.instance_variable_get(:@socket)
      end

      context 'when a non-timeout socket error occurs' do

        before do
          expect(socket).to receive(:write).and_raise(Mongo::Error::SocketError)
        end

        let(:result) do
          expect do
            connection.dispatch([ msg0 ], context)
          end.to raise_error(Mongo::Error::SocketError)
        end

        it 'marks connection perished' do
          result
          expect(connection).to be_error
        end

        context 'in load-balanced topology' do
          require_topology :load_balanced

          it 'disconnects connection pool for service id' do
            connection.global_id.should_not be nil

            RSpec::Mocks.with_temporary_scope do
              expect(server.pool).to receive(:disconnect!).with(
                service_id: connection.service_id
              )
              result
            end
          end

          it 'does not mark server unknown' do
            expect(server).not_to be_unknown
            result
            expect(server).not_to be_unknown
          end
        end

        context 'in non-lb topologies' do
          require_topology :single, :replica_set, :sharded

          it 'disconnects connection pool' do
            expect(server.pool).to receive(:disconnect!)
            result
          end

          it 'marks server unknown' do
            expect(server).not_to be_unknown
            result
            expect(server).to be_unknown
          end
        end

        it 'does not request server scan' do
          expect(server.scan_semaphore).not_to receive(:signal)
          result
        end
      end

      context 'when a socket timeout occurs' do

        before do
          expect(socket).to receive(:write).and_raise(Mongo::Error::SocketTimeoutError)
        end

        let(:result) do
          expect do
            connection.dispatch([ msg0 ], context)
          end.to raise_error(Mongo::Error::SocketTimeoutError)
        end

        it 'marks connection perished' do
          result
          expect(connection).to be_error
        end

=begin These assertions require a working cluster with working SDAM flow, which the tests do not configure
        it 'does not disconnect connection pool' do
          expect(server.pool).not_to receive(:disconnect!)
          result
        end
=end

        it 'does not mark server unknown' do
          expect(server).not_to be_unknown
          result
          expect(server).not_to be_unknown
        end
      end
    end

    context 'when a socket timeout is set on client' do

      let(:connection) do
        described_class.new(server, socket_timeout: 10)
      end

      it 'is propagated to connection timeout' do
        expect(connection.timeout).to eq(10)
      end
    end

    context 'when an operation never completes' do
      let(:client) do
        authorized_client.with(socket_timeout: 1.5,
          # Read retries would cause the reads to be attempted twice,
          # thus making the find take twice as long to time out.
          retry_reads: false, max_read_retries: 0)
      end

      before do
        authorized_collection.insert_one(test: 1)
        client.cluster.next_primary
      end

      it 'times out and raises SocketTimeoutError' do
        start = Mongo::Utils.monotonic_time
        begin
          Timeout::timeout(1.5 + 15) do
            client[authorized_collection.name].find("$where" => "sleep(2000) || true").first
          end
        rescue => ex
          end_time = Mongo::Utils.monotonic_time
          expect(ex).to be_a(Mongo::Error::SocketTimeoutError)
          expect(ex.message).to match(/Took more than 1.5 seconds to receive data/)
        else
          fail 'Expected a timeout'
        end
        # allow 1.5 seconds +- 0.5 seconds
        expect(end_time - start).to be_within(1).of(2)
      end

      context 'when the socket_timeout is negative' do

        let(:connection) do
          described_class.new(server, server.options.merge(connection_pool: pool)).tap do |connection|
            connection.connect!
          end
        end

        before do
          expect(msg0).to receive(:replyable?) { false }
          connection.send(:deliver, msg0, context)

          connection.send(:socket).instance_variable_set(:@timeout, -(Time.now.to_i))
        end

        let(:reply) do
          Mongo::Protocol::Message.deserialize(connection.send(:socket),
            16*1024*1024, msg0.request_id)
        end

        it 'raises a timeout error' do
          expect {
            reply
          }.to raise_exception(Mongo::Error::SocketTimeoutError)
        end
      end
    end
  end

  describe '#initialize' do

    context 'when host and port are provided' do

      let(:connection) do
        described_class.new(server, server.options.merge(connection_pool: pool))
      end

      it 'sets the address' do
        expect(connection.address).to eq(server.address)
      end

      it 'sets id' do
        expect(connection.id).to eq(1)
      end

      context 'multiple connections' do
        it 'use incrementing ids' do
          expect(connection.id).to eq(1)

          second_connection = described_class.new(server, server.options.merge(connection_pool: pool))
          expect(second_connection.id).to eq(2)
        end
      end

      context 'two pools for different servers' do
        let(:server2) do
          register_server(
            Mongo::Server.new(address, cluster, monitoring, listeners,
              server_options.merge(
                load_balancer: ClusterConfig.instance.topology == :load_balanced,
              )
            )
          )
        end

        before do
          allow(server).to receive(:unknown?).and_return(false)
          allow(server2).to receive(:unknown?).and_return(false)
        end

        it 'ids do not share namespace' do
          server.pool.with_connection do |conn|
            expect(conn.id).to eq(1)
          end
          server2.pool.with_connection do |conn|
            expect(conn.id).to eq(1)
          end
        end
      end

      it 'sets the socket to nil' do
        expect(connection.send(:socket)).to be_nil
      end

      context 'when timeout is not set in client options' do
        let(:server_options) do
          SpecConfig.instance.test_options.merge(monitoring_io: false, socket_timeout: nil)
        end

        it 'does not set the timeout to the default' do
          expect(connection.timeout).to be_nil
        end
      end
    end

    context 'when timeout options are provided' do

      let(:connection) do
        described_class.new(server, socket_timeout: 10)
      end

      it 'sets the timeout' do
        expect(connection.timeout).to eq(10)
      end
    end

    context 'when ssl options are provided' do

      let(:ssl_options) do
        { :ssl => true, :ssl_key => 'file', :ssl_key_pass_phrase => 'iamaphrase' }
      end

      let(:connection) do
        described_class.new(server, ssl_options)
      end

      it 'sets the ssl options' do
        expect(connection.send(:ssl_options)).to eq(ssl_options)
      end
    end

    context 'when ssl is false' do

      context 'when ssl options are provided' do

        let(:ssl_options) do
          { :ssl => false, :ssl_key => 'file', :ssl_key_pass_phrase => 'iamaphrase' }
        end

        let(:connection) do
          described_class.new(server, ssl_options)
        end

        it 'does not set the ssl options' do
          expect(connection.send(:ssl_options)).to eq(ssl: false)
        end
      end

      context 'when ssl options are not provided' do

        let(:ssl_options) do
          { :ssl => false }
        end

        let(:connection) do
          described_class.new(server, ssl_options)
        end

        it 'does not set the ssl options' do
          expect(connection.send(:ssl_options)).to eq(ssl: false)
        end
      end
    end

    context 'when authentication options are provided' do
      require_no_external_user

      let(:connection) do
        described_class.new(
          server,
          user: SpecConfig.instance.test_user.name,
          password: SpecConfig.instance.test_user.password,
          database: SpecConfig.instance.test_db,
          auth_mech: :mongodb_cr,
          connection_pool: pool,
        )
      end

      let(:user) do
        Mongo::Auth::User.new(
          database: SpecConfig.instance.test_db,
          user: SpecConfig.instance.test_user.name,
          password: SpecConfig.instance.test_user.password
        )
      end

      it 'sets the auth options' do
        expect(connection.options[:user]).to eq(user.name)
      end
    end
  end

  context 'when different timeout options are set' do

    let(:client) do
      authorized_client.with(options)
    end

    let(:server) do
      client.cluster.next_primary
    end

    let(:address) do
      server.address
    end

    let(:connection) do
      described_class.new(server, server.options.merge(connection_pool: pool))
    end

    context 'when a connect_timeout is in the options' do

      context 'when a socket_timeout is in the options' do

        let(:options) do
          SpecConfig.instance.test_options.merge(connect_timeout: 3, socket_timeout: 5)
        end

        before do
          connection.connect!
        end

        it 'uses the connect_timeout for the address' do
          expect(connection.address.options[:connect_timeout]).to eq(3)
        end

        it 'uses the socket_timeout as the socket_timeout' do
          expect(connection.send(:socket).timeout).to eq(5)
        end
      end

      context 'when a socket_timeout is not in the options' do

        let(:options) do
          SpecConfig.instance.test_options.merge(connect_timeout: 3, socket_timeout: nil)
        end

        before do
          connection.connect!
        end

        it 'uses the connect_timeout for the address' do
          expect(connection.address.options[:connect_timeout]).to eq(3)
        end

        it 'does not use a socket_timeout' do
          expect(connection.send(:socket).timeout).to be(nil)
        end
      end
    end

    context 'when a connect_timeout is not in the options' do

      context 'when a socket_timeout is in the options' do

        let(:options) do
          SpecConfig.instance.test_options.merge(connect_timeout: nil, socket_timeout: 5)
        end

        before do
          connection.connect!
        end

        it 'does not specify connect_timeout for the address' do
          expect(connection.address.options[:connect_timeout]).to be nil
        end

        it 'uses the socket_timeout' do
          expect(connection.send(:socket).timeout).to eq(5)
        end
      end

      context 'when a socket_timeout is not in the options' do

        let(:options) do
          SpecConfig.instance.test_options.merge(connect_timeout: nil, socket_timeout: nil)
        end

        before do
          connection.connect!
        end

        it 'does not specify connect_timeout for the address' do
          expect(connection.address.options[:connect_timeout]).to be nil
        end

        it 'does not use a socket_timeout' do
          expect(connection.send(:socket).timeout).to be(nil)
        end
      end
    end
  end

  describe '#app_metadata' do
    context 'when all options are identical to server' do
      let(:connection) do
        described_class.new(server, server.options.merge(connection_pool: pool))
      end

      it 'is the same object as server app_metadata' do
        expect(connection.app_metadata).not_to be nil
        expect(connection.app_metadata).to be server.app_metadata
      end
    end

    context 'when auth options are identical to server' do
      let(:connection) do
        described_class.new(server, server.options.merge(socket_timeout: 2, connection_pool: pool))
      end

      it 'is the same object as server app_metadata' do
        expect(connection.app_metadata).not_to be nil
        expect(connection.app_metadata).to be server.app_metadata
      end
    end

    context 'when auth options differ from server' do
      require_no_external_user

      let(:connection) do
        described_class.new(server, server.options.merge(user: 'foo', connection_pool: pool))
      end

      it 'is different object from server app_metadata' do
        expect(connection.app_metadata).not_to be nil
        expect(connection.app_metadata).not_to be server.app_metadata
      end

      it 'includes request auth mechanism' do
        document = connection.app_metadata.send(:document)
        expect(document[:saslSupportedMechs]).to eq('admin.foo')
      end
    end
  end

  describe '#generation' do

    context 'non-lb' do
      require_topology :single, :replica_set, :sharded

      before do
        allow(server).to receive(:unknown?).and_return(false)
      end

      it 'is set' do
        server.with_connection do |conn|
          conn.service_id.should be nil
          conn.generation.should be_a(Integer)
        end
      end

      context 'clean slate' do
        clean_slate

        before do
          allow(server).to receive(:unknown?).and_return(false)
        end

        it 'starts from 1' do
          server.with_connection do |conn|
            conn.service_id.should be nil
            conn.generation.should == 1
          end
        end
      end
    end

    context 'lb' do
      require_topology :load_balanced

      it 'is set' do
        server.with_connection do |conn|
          conn.service_id.should_not be nil
          conn.generation.should be_a(Integer)
        end
      end

      context 'clean slate' do
        clean_slate

        it 'starts from 1' do
          server.with_connection do |conn|
            conn.service_id.should_not be nil
            conn.generation.should == 1
          end
        end
      end
    end
  end
end
