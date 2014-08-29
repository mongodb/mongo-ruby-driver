require 'spec_helper'

describe Mongo::Server do

  describe '#==' do

    let(:server) do
      described_class.new('127.0.0.1:27017')
    end

    context 'when the other is not a server' do

      let(:other) do
        false
      end

      it 'returns false' do
        expect(server).to_not eq(other)
      end
    end

    context 'when the other is a server' do

      context 'when the addresses match' do

        let(:other) do
          described_class.new('127.0.0.1:27017')
        end

        it 'returns true' do
          expect(server).to eq(other)
        end
      end

      context 'when the addresses dont match', simulator: 'cluster' do

        let(:other) do
          described_class.new('127.0.0.1:27018')
        end

        it 'returns false' do
          expect(server).to_not eq(other)
        end
      end
    end
  end

  describe '#context' do

    let(:server) do
      described_class.new('127.0.0.1:27017')
    end

    let(:context) do
      server.context
    end

    it 'returns a new server context' do
      expect(context.server).to eq(server)
    end
  end

  describe '#initialize' do

    let(:address) do
      '127.0.0.1:27017'
    end

    let(:server) do
      described_class.new(address, :heartbeat_frequency => 5)
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
      expect(server.options).to eq(:heartbeat_frequency => 5)
    end
  end

  describe '#pool' do

    let(:server) do
      described_class.new('127.0.0.1:27017')
    end

    let(:pool) do
      server.pool
    end

    it 'returns the connection pool for the server' do
      expect(pool.pool_size).to eq(5)
    end
  end
end
