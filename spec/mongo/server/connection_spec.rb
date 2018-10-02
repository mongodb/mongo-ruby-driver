require 'spec_helper'

describe Mongo::Server::Connection do
  class ConnectionSpecTestException < Exception; end

  let(:address) do
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
      pool = double('pool')
      allow(pool).to receive(:disconnect!)
      allow(cl).to receive(:pool).and_return(pool)
    end
  end

  declare_topology_double

  let(:server) do
    Mongo::Server.new(address, cluster, monitoring, listeners, SpecConfig.instance.test_options)
  end

  let(:pool) do
    double('pool')
  end

  after do
    allow(cluster).to receive(:pool).with(server).and_return(pool)
    allow(pool).to receive(:disconnect!).and_return(true)
    server.disconnect!
  end

  describe '#connect!' do

    context 'when no socket exists' do

      let(:connection) do
        described_class.new(server, server.options)
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
          expect(result).to be_a(Exception)
        end

        it 'clears socket' do
          result
          expect(connection.send(:socket)).to be nil
        end

        it 'attempts to reconnect after failure when asked' do
          # for some reason referencing result here instead of
          # copy pasting it like this doesn't work
          expect(connection).to receive(:authenticate!).and_raise(exception)
          expect do
            connection.connect!
          end.to raise_error(exception)

          expect(connection).to receive(:authenticate!).and_raise(ConnectionSpecTestException)
          expect do
            connection.connect!
          end.to raise_error(ConnectionSpecTestException)
        end
      end

      context 'when #handshake! raises an exception' do
        let(:exception) do
          Mongo::Error::SocketError.new
        end

        let(:result) do
          expect(connection).to receive(:handshake!).and_raise(exception)
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

      context 'when #authenticate! raises an exception' do
        let(:exception) do
          Mongo::Error::OperationFailure.new
        end

        let(:result) do
          expect(connection).to receive(:authenticate!).and_raise(exception)
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

      context 'when a non-Mongo exception is raised' do
        let(:exception) do
          SystemExit.new
        end

        let(:result) do
          expect(connection).to receive(:authenticate!).and_raise(exception)
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
        described_class.new(server, server.options)
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

    context 'when user credentials exist' do

      context 'when the user is not authorized' do

        let(:connection) do
          described_class.new(
            server,
            SpecConfig.instance.test_options.merge(
              :user => 'notauser',
              :password => 'password',
              :database => SpecConfig.instance.test_db,
              :heartbeat_frequency => 30)
          )
        end

        let(:error) do
          e = begin; connection.send(:ensure_connected); rescue => ex; ex; end
        end

        context 'not checking pool disconnection' do
          before do
            allow(cluster).to receive(:pool).with(server).and_return(pool)
            allow(pool).to receive(:disconnect!).and_return(true)
          end

          it 'raises an error' do
            expect(error).to be_a(Mongo::Auth::Unauthorized)
          end

          it 'disconnects the socket' do
            error
            expect(connection.send(:socket)).to be(nil)
          end

          it "keeps server's type" do
            old_type = server.description.server_type
            expect(old_type).not_to eq(:unknown)
            old_oid = server.description.object_id
            error
            expect(server.description.server_type).to eq(old_type)
            expect(server.description.object_id).to eq(old_oid)
          end

          it "keeps topology" do
            old_topology = server.cluster.topology
            expect(old_topology).not_to be(Mongo::Cluster::Topology::Unknown)
            error
            expect(server.cluster.topology).to eql(old_topology)
          end
        end

        # need a separate context here, otherwise disconnect expectation
        # is ignored due to allowing disconnects in the other context
        context 'checking pool disconnection' do
          it 'disconnects non-monitoring sockets' do
            expect(cluster).to receive(:pool).with(server).and_return(pool)
            expect(pool).to receive(:disconnect!).and_return(true)
            error
          end
        end
      end

      describe 'when the user is authorized' do

        let(:connection) do
          described_class.new(
            server,
            SpecConfig.instance.test_options.merge(
              :user => SpecConfig.instance.test_user.name,
              :password => SpecConfig.instance.test_user.password,
              :database => SpecConfig.instance.test_user.database )
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

  end

  describe '#disconnect!' do

    context 'when a socket is not connected' do

      let(:connection) do
        described_class.new(server, server.options)
      end

      it 'does not raise an error' do
        expect(connection.disconnect!).to be true
      end
    end

    context 'when a socket is connected' do

      let(:connection) do
        described_class.new(server, server.options)
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

    let!(:connection) do
      described_class.new(
        server,
        SpecConfig.instance.test_options.merge(
          :user => SpecConfig.instance.test_user.name,
          :password => SpecConfig.instance.test_user.password,
          :database => SpecConfig.instance.test_user.database )
      )
    end

    let(:documents) do
      [{ 'name' => 'testing' }]
    end

    let(:insert) do
      Mongo::Protocol::Insert.new(SpecConfig.instance.test_db, TEST_COLL, documents)
    end

    let(:query) do
      Mongo::Protocol::Query.new(SpecConfig.instance.test_db, TEST_COLL, { 'name' => 'testing' })
    end

    context 'when providing a single message' do

      let(:reply) do
        connection.dispatch([ insert, query ])
      end

      after do
        authorized_collection.delete_many
      end

      it 'it dispatchs the message to the socket' do
        expect(reply.documents.first['name']).to eq('testing')
      end
    end

    context 'when providing multiple messages' do

      let(:selector) do
        { :getlasterror => 1 }
      end

      let(:command) do
        Mongo::Protocol::Query.new(SpecConfig.instance.test_db, '$cmd', selector, :limit => -1)
      end

      let(:reply) do
        connection.dispatch([ insert, command ])
      end

      after do
        authorized_collection.delete_many
      end

      it 'it dispatchs the message to the socket' do
        expect(reply.documents.first['ok']).to eq(1.0)
      end
    end

    context 'when the response_to does not match the request_id' do

      let(:documents) do
        [{ 'name' => 'bob' }, { 'name' => 'alice' }]
      end

      let(:insert) do
        Mongo::Protocol::Insert.new(SpecConfig.instance.test_db, TEST_COLL, documents)
      end

      let(:query_bob) do
        Mongo::Protocol::Query.new(SpecConfig.instance.test_db, TEST_COLL, { name: 'bob' })
      end

      let(:query_alice) do
        Mongo::Protocol::Query.new(SpecConfig.instance.test_db, TEST_COLL, { name: 'alice' })
      end

      after do
        authorized_collection.delete_many
      end

      before do
        # Fake a query for which we did not read the response. See RUBY-1117
        allow(query_bob).to receive(:replyable?) { false }
        connection.dispatch([ insert, query_bob ])
      end

      it 'raises an UnexpectedResponse error' do
        expect {
          connection.dispatch([ query_alice ])
        }.to raise_error(Mongo::Error::UnexpectedResponse,
          /Got response for request ID \d+ but expected response for request ID \d+/)
      end

      it 'does not affect subsequent requests' do
        expect {
          connection.dispatch([ query_alice ])
        }.to raise_error(Mongo::Error::UnexpectedResponse)

        docs = connection.dispatch([ query_alice ]).documents
        expect(docs).to_not be_empty
        expect(docs.first['name']).to eq('alice')
      end
    end

    context 'when a request is interrupted (Thread.kill)' do

      let(:documents) do
        [{ 'name' => 'bob' }, { 'name' => 'alice' }]
      end

      let(:insert) do
        Mongo::Protocol::Insert.new(SpecConfig.instance.test_db, TEST_COLL, documents)
      end

      let(:query_bob) do
        Mongo::Protocol::Query.new(SpecConfig.instance.test_db, TEST_COLL, { name: 'bob' })
      end

      let(:query_alice) do
        Mongo::Protocol::Query.new(SpecConfig.instance.test_db, TEST_COLL, { name: 'alice' })
      end

      before do
        connection.dispatch([ insert ])
      end

      after do
        authorized_collection.delete_many
      end

      it 'closes the socket and does not use it for subsequent requests' do
        t = Thread.new {
          # Kill the thread just before the reply is read
          allow(Mongo::Protocol::Reply).to receive(:deserialize_header) { t.kill and t.stop? }
          connection.dispatch([ query_bob ])
        }
        t.join
        allow(Mongo::Protocol::Message).to receive(:deserialize_header).and_call_original
        expect(connection.dispatch([ query_alice ]).documents.first['name']).to eq('alice')
      end
    end

    context 'when the message exceeds the max size' do

      context 'when the message is an insert' do

        before do
          allow(connection).to receive(:max_message_size).and_return(200)
        end

        let(:documents) do
          [{ 'name' => 'testing' } ] * 10
        end

        let(:reply) do
          connection.dispatch([ insert ])
        end

        it 'checks the size against the max message size' do
          expect {
            reply
          }.to raise_exception(Mongo::Error::MaxMessageSize)
        end
      end

      context 'when the message is a command' do

        before do
          allow(connection).to receive(:max_bson_object_size).and_return(100)
        end

        let(:selector) do
          { :getlasterror => '1' }
        end

        let(:command) do
          Mongo::Protocol::Query.new(SpecConfig.instance.test_db, '$cmd', selector, :limit => -1)
        end

        let(:reply) do
          connection.dispatch([ command ])
        end

        it 'checks the size against the max bson size' do
          expect {
            reply
          }.to raise_exception(Mongo::Error::MaxBSONSize)
        end
      end
    end

    context 'when a network or socket error occurs' do

      let(:socket) do
        connection.connect!
        connection.instance_variable_get(:@socket)
      end

      before do
        expect(socket).to receive(:write).and_raise(Mongo::Error::SocketError)
      end

      it 'disconnects and raises the exception' do
        expect {
          connection.dispatch([ insert ])
        }.to raise_error(Mongo::Error::SocketError)
        expect(connection).to_not be_connected
      end
    end

    context 'when a socket timeout is set' do

      let(:connection) do
        described_class.new(server, socket_timeout: 10)
      end

      it 'sets the timeout' do
        expect(connection.timeout).to eq(10)
      end

      let(:client) do
        authorized_client.with(socket_timeout: 1.5)
      end

      before do
        authorized_collection.insert_one(a: 1)
      end

      after do
        sleep(0.5)
        authorized_collection.delete_many
        client.close
      end

      it 'raises a timeout when it expires' do
        start = Time.now
        begin
          Timeout::timeout(1.5 + 2) do
            client[authorized_collection.name].find("$where" => "sleep(2000) || true").first
          end
        rescue => ex
          end_time = Time.now
          expect(ex).to be_a(Timeout::Error)
          expect(ex.message).to eq("Took more than 1.5 seconds to receive data.")
        end
        # Account for wait queue timeout (2s) and rescue
        expect(end_time - start).to be_within(2.5).of(1.5)
      end

      context 'when the socket_timeout is negative' do

        let(:connection) do
          described_class.new(server, server.options)
        end

        let(:messages) do
          [ insert ]
        end

        before do
          connection.send(:write, messages)
          connection.send(:socket).instance_variable_set(:@timeout, -(Time.now.to_i))
        end

        let(:reply) do
          connection.send(:read, messages.last.request_id)
        end

        it 'raises a timeout error' do
          expect {
            reply
          }.to raise_exception(Timeout::Error)
        end
      end
    end

    context 'when the process is forked' do

      let(:insert) do
        Mongo::Protocol::Insert.new(SpecConfig.instance.test_db, TEST_COLL, documents)
      end

      before do
        expect(Process).to receive(:pid).at_least(:once).and_return(1)
      end

      after do
        authorized_collection.delete_many
      end

      it 'disconnects the connection' do
        expect(connection).to receive(:disconnect!).and_call_original
        connection.dispatch([ insert ])
      end

      it 'sets a new pid' do
        connection.dispatch([ insert ])
        expect(connection.pid).to eq(1)
      end
    end
  end

  describe '#initialize' do

    context 'when host and port are provided' do

      let(:connection) do
        described_class.new(server, server.options)
      end

      it 'sets the address' do
        expect(connection.address).to eq(server.address)
      end

      it 'sets the socket to nil' do
        expect(connection.send(:socket)).to be_nil
      end

      it 'does not set the timeout to the default' do
        expect(connection.timeout).to be_nil
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
          expect(connection.send(:ssl_options)).to be_empty
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
          expect(connection.send(:ssl_options)).to be_empty
        end
      end
    end

    context 'when authentication options are provided' do

      let(:connection) do
        described_class.new(
          server,
          :user => SpecConfig.instance.test_user.name,
          :password => SpecConfig.instance.test_user.password,
          :database => SpecConfig.instance.test_db,
          :auth_mech => :mongodb_cr
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

  describe '#auth_mechanism' do

    let(:connection) do
      described_class.new(server, server.options)
    end

    let(:reply) do
      double('reply').tap do |r|
        allow(r).to receive(:documents).and_return([ ismaster ])
      end
    end

    before do
      connection.connect!
    end

    context 'when the ismaster response indicates the auth mechanism is :scram' do

      let(:features) do
        Mongo::Server::Description::Features.new(0..7)
      end

      context 'when the server auth mechanism is scram', if: scram_sha_1_enabled? && !scram_sha_256_enabled? do

        it 'uses scram' do
          allow(Mongo::Server::Description::Features).to receive(:new).and_return(features)
          connection.send(:handshake!)
          expect(connection.send(:default_mechanism)).to eq(:scram)
        end
      end

      context 'when the server auth mechanism is the default (mongodb_cr)', unless: scram_sha_1_enabled?  do

        it 'uses scram' do
          allow(Mongo::Server::Description::Features).to receive(:new).and_return(features)
          connection.send(:handshake!)
          expect(connection.send(:default_mechanism)).to eq(:scram)
        end
      end
    end

    context 'when the ismaster response indicates the auth mechanism is :mongodb_cr' do

      let(:features) do
        Mongo::Server::Description::Features.new(0..2)
      end

      context 'when the server auth mechanism is scram', if: scram_sha_1_enabled? && !scram_sha_256_enabled? do

        it 'uses scram' do
          allow(Mongo::Server::Description::Features).to receive(:new).and_return(features)
          connection.send(:handshake!)
          expect(connection.send(:default_mechanism)).to eq(:scram)
        end
      end

      context 'when the server auth mechanism is the default (mongodb_cr)', unless: scram_sha_1_enabled?  do

        it 'uses mongodb_cr' do
          allow(Mongo::Server::Description::Features).to receive(:new).and_return(features)
          connection.send(:handshake!)
          expect(connection.send(:default_mechanism)).to eq(:mongodb_cr)
        end
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
      described_class.new(server, server.options)
    end

    after do
      client.close
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
          expect(connection.address.send(:connect_timeout)).to eq(3)
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
          expect(connection.address.send(:connect_timeout)).to eq(3)
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

        it 'uses the default connect_timeout for the address' do
          expect(connection.address.send(:connect_timeout)).to eq(10)
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

        it 'uses the default connect_timeout for the address' do
          expect(connection.address.send(:connect_timeout)).to eq(10)
        end

        it 'does not use a socket_timeout' do
          expect(connection.send(:socket).timeout).to be(nil)
        end
      end
    end
  end
end
