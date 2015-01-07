require 'spec_helper'

describe Mongo::Server do

  let(:listeners) do
    Mongo::Event::Listeners.new
  end

  describe '#==' do

    let(:server) do
      described_class.new('127.0.0.1:27017', listeners)
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
          described_class.new('127.0.0.1:27017', listeners)
        end

        it 'returns true' do
          expect(server).to eq(other)
        end
      end

      context 'when the addresses dont match' do

        let(:other) do
          described_class.new('127.0.0.1:27018', listeners)
        end

        it 'returns false' do
          expect(server).to_not eq(other)
        end
      end
    end
  end

  describe '#context' do

    let(:server) do
      described_class.new('127.0.0.1:27017', listeners)
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
      described_class.new(address, listeners, :heartbeat_frequency => 5)
    end

    it 'sets the address host' do
      expect(server.address.host).to eq('127.0.0.1')
    end

    it 'sets the address port' do
      expect(server.address.port).to eq(27017)
    end

    it 'sets the options' do
      expect(server.options).to eq(:heartbeat_frequency => 5)
    end
  end

  describe '#pool' do

    let(:server) do
      described_class.new('127.0.0.1:27017', listeners)
    end

    let(:pool) do
      server.pool
    end

    it 'returns the connection pool for the server' do
      expect(pool).to be_a(Mongo::Pool)
    end
  end

  describe '#disconnect!' do

    let(:server) do
      described_class.new('127.0.0.1:27017', listeners)
    end

    it 'removed the monitor thread instance' do
      s = described_class.new('127.0.0.1:27017', listeners)
      monitor_count = Mongo::Server::Monitor.threads.size
      s.disconnect!
      expect(Mongo::Server::Monitor.threads.size).to eq(monitor_count - 1)
    end

    it 'stops the monitor instance' do
      expect_any_instance_of(Mongo::Server::Monitor).to receive(:stop).and_return(true)
      server.disconnect!
    end
  end
end
