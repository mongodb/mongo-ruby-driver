require 'spec_helper'

describe Mongo::Connection do

  let(:address) do
    Mongo::Server::Address.new('127.0.0.1:27017')
  end

  describe '#connect!' do

    let(:connection) do
      described_class.new(address)
    end

    context 'when no socket exists' do

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
  end

  describe '#disconnect!' do

    context 'when a socket is not connected' do

      let(:connection) do
        described_class.new(address)
      end

      it 'does not raise an error' do
        expect(connection.disconnect!).to be true
      end
    end

    context 'when a socket is connected' do

      let(:connection) do
        described_class.new(address)
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

  describe '#initialize' do

    context 'when host and port are provided' do

      let(:connection) do
        described_class.new(address)
      end

      it 'sets the address' do
        expect(connection.address).to eq(address)
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
        described_class.new(address, 10)
      end

      it 'sets the timeout' do
        expect(connection.timeout).to eq(10)
      end
    end

    context 'when ssl options are provided' do

      let(:connection) do
        described_class.new(address, nil, :ssl => true)
      end

      it 'sets the ssl options' do
        expect(connection.send(:ssl_opts)).to eq(:ssl => true)
      end
    end
  end

  describe '#read' do

    let(:connection) do
      described_class.new(address, 5)
    end

    let(:documents) do
      [{ 'name' => 'testing' }]
    end

    let(:insert) do
      Mongo::Protocol::Insert.new('mongo_test', 'users', documents)
    end

    let(:query) do
      Mongo::Protocol::Query.new('mongo_test', 'users', {})
    end

    let(:delete) do
      Mongo::Protocol::Delete.new('mongo_test', 'users', {})
    end

    before do
      connection.write([ insert ])
      connection.write([ query ])
    end

    # @todo: Can remove this once we have more implemented with global hooks.
    after do
      connection.write([ delete ])
    end

    let(:reply) do
      connection.read
    end

    it 'returns the reply from the connection' do
      expect(reply.documents.first['name']).to eq('testing')
    end
  end

  describe '#write' do

    let(:connection) do
      described_class.new(address, 5)
    end

    let(:documents) do
      [{ 'name' => 'testing' }]
    end

    let(:insert) do
      Mongo::Protocol::Insert.new('mongo_test', 'users', documents)
    end

    let(:query) do
      Mongo::Protocol::Query.new('mongo_test', 'users', {})
    end

    let(:delete) do
      Mongo::Protocol::Delete.new('mongo_test', 'users', {})
    end

    context 'when providing a single message' do

      before do
        connection.write([ insert ])
        connection.write([ query ])
      end

      # @todo: Can remove this once we have more implemented with global hooks.
      after do
        connection.write([ delete ])
      end

      it 'it writes the message to the socket' do
        expect(connection.read.documents.first['name']).to eq('testing')
      end
    end

    context 'when providing multiple messages' do

      let(:selector) do
        { :getlasterror => 1 }
      end

      let(:command) do
        Mongo::Protocol::Query.new('mongo_test', '$cmd', selector, :limit => -1)
      end

      before do
        connection.write([ insert, command ])
      end

      # @todo: Can remove this once we have more implemented with global hooks.
      after do
        connection.write([ delete ])
      end

      it 'it writes the message to the socket' do
        expect(connection.read.documents.first['ok']).to eq(1.0)
      end
    end
  end
end
