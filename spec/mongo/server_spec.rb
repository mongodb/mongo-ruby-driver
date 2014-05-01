require 'spec_helper'

describe Mongo::Server do

  describe '#dispatch' do

    let(:server) do
      described_class.new('127.0.0.1:27017')
    end

    let(:documents) do
      [{ 'name' => 'testing' }]
    end

    let(:insert) do
      Mongo::Protocol::Insert.new(TEST_DB, TEST_COLL, documents)
    end

    let(:query) do
      Mongo::Protocol::Query.new(TEST_DB, TEST_COLL, {})
    end

    let(:delete) do
      Mongo::Protocol::Delete.new(TEST_DB, TEST_COLL, {})
    end

    context 'when the server description is not set' do

      before do
        server.dispatch([ insert ])
      end

      it 'sets the server description' do
        expect(server.description).to be_primary
      end
    end

    context 'when providing a single message' do

      before do
        server.dispatch([ insert ])
      end

      let(:reply) do
        server.dispatch([ query ])
      end

      # @todo: Can remove this once we have more implemented with global hooks.
      after do
        server.dispatch([ delete ])
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
        server.dispatch([ insert, command ])
      end

      # @todo: Can remove this once we have more implemented with global hooks.
      after do
        server.dispatch([ delete ])
      end

      it 'it dispatchs the message to the socket' do
        expect(reply.documents.first['ok']).to eq(1.0)
      end
    end
  end

  describe '#initialize' do

    let(:address) do
      '127.0.0.1:27017'
    end

    let(:server) do
      described_class.new(address, :refresh_interval => 5)
    end

    it 'sets the address host' do
      expect(server.address.host).to eq('127.0.0.1')
    end

    it 'sets the address port' do
      expect(server.address.port).to eq(27017)
    end

    it 'sets the address ip' do
      expect(server.address.ip).to eq('127.0.0.1')
    end

    it 'sets the options' do
      expect(server.options).to eq(:refresh_interval => 5)
    end
  end

  describe '#refresh!' do

    let(:address) do
      '127.0.0.1:27017'
    end

    context 'when the server is a single server' do

      let(:server) do
        described_class.new(address)
      end

      context 'when the server is available' do

        it 'flags the server as master' do

        end

        it 'flags the mode as operable' do

        end

        it 'sets the server latency' do

        end
      end

      context 'when the server is down' do

        it 'flags the server as down' do

        end

        it 'does not flag the server as operable' do

        end

        it 'removes the server latency' do

        end
      end
    end

    context 'when the server is a replica set' do

    end

    context 'when the server is mongos' do

    end
  end

  describe '#refresh_interval' do

    let(:address) do
      '127.0.0.1:27017'
    end

    context 'when an option is provided' do

      let(:server) do
        described_class.new(address, :refresh_interval => 10)
      end

      it 'returns the option' do
        expect(server.refresh_interval).to eq(10)
      end
    end

    context 'when no option is provided' do

      let(:server) do
        described_class.new(address)
      end

      it 'defaults to 5' do
        expect(server.refresh_interval).to eq(5)
      end
    end
  end
end
