require 'spec_helper'

describe Mongo::Connection do

  let(:server) do
    Mongo::Server.new('127.0.0.1:27017', Mongo::Event::Listeners.new)
  end

  describe '#connect!' do

    context 'when no socket exists' do

      let(:connection) do
        described_class.new(server)
      end

      let!(:result) do
        connection.connect!
      end

      let(:socket) do
        connection.send(:socket)
      end

      it 'returns true' do
        expect(result).to be true
      end

      it 'creates a socket' do
        expect(socket).to_not be_nil
      end

      it 'connects the socket' do
        expect(socket).to be_alive
      end
    end

    context 'when a socket exists' do

      let(:connection) do
        described_class.new(server)
      end

      before do
        connection.connect!
        connection.connect!
      end

      let(:socket) do
        connection.send(:socket)
      end

      it 'keeps the socket alive' do
        expect(socket).to be_alive
      end
    end

    context 'when user credentials exist' do

      context 'when the user is not authorized' do

        let(:connection) do
          described_class.new(
            server,
            :user => 'notauser',
            :password => 'password',
            :database => TEST_DB
          )
        end

        it 'raises an error' do
          expect {
            connection.connect!
          }.to raise_error(Mongo::Auth::Unauthorized)
        end
      end

      describe 'when the user is authorized' do

        let(:connection) do
          described_class.new(
            server,
            :user => TEST_USER.name,
            :password => TEST_USER.password,
            :database => TEST_DB
          )
        end

        before do
          connection.connect!
        end

        it 'sets the connection as authenticated' do
          expect(connection).to be_authenticated
        end
      end
    end
  end

  describe '#disconnect!' do

    context 'when a socket is not connected' do

      let(:connection) do
        described_class.new(server)
      end

      it 'does not raise an error' do
        expect(connection.disconnect!).to be true
      end
    end

    context 'when a socket is connected' do

      let(:connection) do
        described_class.new(server)
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
        :user => TEST_USER.name,
        :password => TEST_USER.password,
        :database => TEST_DB
      )
    end

    let(:documents) do
      [{ 'name' => 'testing' }]
    end

    let(:insert) do
      Mongo::Protocol::Insert.new(TEST_DB, TEST_COLL, documents)
    end

    let(:query) do
      Mongo::Protocol::Query.new(TEST_DB, TEST_COLL, { 'name' => 'testing' })
    end

    let(:delete) do
      Mongo::Protocol::Delete.new(TEST_DB, TEST_COLL, {})
    end

    context 'when providing a single message' do

      let(:reply) do
        connection.dispatch([ insert, query ])
      end

      # @todo: Can remove this once we have more implemented with global hooks.
      after do
        connection.dispatch([ delete ])
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
        Mongo::Protocol::Query.new(TEST_DB, '$cmd', selector, :limit => -1)
      end

      let(:reply) do
        connection.dispatch([ insert, command ])
      end

      # @todo: Can remove this once we have more implemented with global hooks.
      after do
        connection.dispatch([ delete ])
      end

      it 'it dispatchs the message to the socket' do
        expect(reply.documents.first['ok']).to eq(1.0)
      end
    end
  end

  describe '#initialize' do

    context 'when host and port are provided' do

      let(:connection) do
        described_class.new(server)
      end

      it 'sets the address' do
        expect(connection.address).to eq(server.address)
      end

      it 'sets the socket to nil' do
        expect(connection.send(:socket)).to be_nil
      end

      it 'sets the timeout to the default' do
        expect(connection.timeout).to eq(5)
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

      let(:connection) do
        described_class.new(server, :ssl => true)
      end

      it 'sets the ssl options' do
        expect(connection.send(:ssl_options)).to eq(:ssl => true)
      end
    end

    context 'when authentication options are provided' do

      let(:connection) do
        described_class.new(
          server,
          :user => TEST_USER.name,
          :password => TEST_USER.password,
          :database => TEST_DB,
          :auth_mech => :mongodb_cr
        )
      end

      let(:user) do
        Mongo::Auth::User.new(
          database: TEST_DB,
          user: TEST_USER.name,
          password: TEST_USER.password
        )
      end

      it 'sets the authentication strategy for the connection' do
        expect(connection.authenticator.user).to eq(user)
      end
    end
  end
end
