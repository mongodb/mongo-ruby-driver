require 'spec_helper'

describe Mongo::Server do

  let(:cluster) do
    double('cluster')
  end

  let(:listeners) do
    Mongo::Event::Listeners.new
  end

  let(:address) do
    Mongo::Address.new('127.0.0.1:27017')
  end

  describe '#==' do

    let(:server) do
      described_class.new(address, cluster, listeners, TEST_OPTIONS)
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
          described_class.new(address, cluster, listeners, TEST_OPTIONS)
        end

        it 'returns true' do
          expect(server).to eq(other)
        end
      end

      context 'when the addresses dont match' do

        let(:other_address) do
          Mongo::Address.new('127.0.0.1:27018')
        end

        let(:other) do
          described_class.new(other_address, cluster, listeners, TEST_OPTIONS)
        end

        it 'returns false' do
          expect(server).to_not eq(other)
        end
      end
    end
  end

  describe '#context' do

    let(:server) do
      described_class.new(address, cluster, listeners, TEST_OPTIONS)
    end

    let(:context) do
      server.context
    end

    it 'returns a new server context' do
      expect(context.server).to eq(server)
    end
  end

  describe '#disconnect!' do

    let(:server) do
      described_class.new(address, cluster, listeners, TEST_OPTIONS)
    end

    it 'stops the monitor instance' do
      expect(server.instance_variable_get(:@monitor)).to receive(:stop!).and_return(true)
      server.disconnect!
    end
  end

  describe '#initialize' do

    let(:server) do
      described_class.new(address, cluster, listeners, TEST_OPTIONS.merge(:heartbeat_frequency => 5))
    end

    it 'sets the address host' do
      expect(server.address.host).to eq('127.0.0.1')
    end

    it 'sets the address port' do
      expect(server.address.port).to eq(27017)
    end

    it 'sets the options' do
      expect(server.options).to eq(TEST_OPTIONS.merge(:heartbeat_frequency => 5))
    end
  end

  describe '#pool' do

    let(:server) do
      described_class.new(address, cluster, listeners, TEST_OPTIONS)
    end

    let(:pool) do
      server.pool
    end

    it 'returns the connection pool for the server' do
      expect(pool).to be_a(Mongo::Server::ConnectionPool)
    end
  end

  describe '#scan!' do

    let(:server) do
      described_class.new(address, cluster, listeners, TEST_OPTIONS)
    end

    it 'forces a scan on the monitor' do
      expect(server.scan!).to eq(server.description)
    end
  end
end
