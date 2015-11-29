require 'spec_helper'

describe Mongo::Server do

  let(:cluster) do
    double('cluster')
  end

  let(:listeners) do
    Mongo::Event::Listeners.new
  end

  let(:monitoring) do
    Mongo::Monitoring.new
  end

  let(:address) do
    Mongo::Address.new('127.0.0.1:27017')
  end

  let(:pool) do
    Mongo::Server::ConnectionPool.get(server)
  end

  describe '#==' do

    let(:server) do
      described_class.new(address, cluster, monitoring, listeners, TEST_OPTIONS)
    end

    after do
      expect(cluster).to receive(:pool).with(server).and_return(pool)
      server.disconnect!
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
          described_class.new(address, cluster, monitoring, listeners, TEST_OPTIONS)
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
          described_class.new(other_address, cluster, monitoring, listeners, TEST_OPTIONS)
        end

        it 'returns false' do
          expect(server).to_not eq(other)
        end
      end
    end
  end

  describe '#connectable?' do

    context 'when the server is connectable' do

      let(:server) do
        described_class.new(address, cluster, monitoring, listeners, TEST_OPTIONS)
      end

      after do
        server.disconnect!
      end

      before do
        expect(cluster).to receive(:pool).with(server).and_return(pool)
      end

      it 'returns true' do
        expect(server).to be_connectable
      end
    end

    context 'when the server is not connectable' do

      let(:bad_address) do
        Mongo::Address.new('127.0.0.1:666')
      end

      let(:server) do
        described_class.new(bad_address, cluster, monitoring, listeners, TEST_OPTIONS)
      end

      before do
        expect(cluster).to receive(:pool).with(server).and_return(pool)
        server.disconnect!
      end

      it 'returns false' do
        expect(server).to_not be_connectable
      end
    end
  end

  describe '#context' do

    let(:server) do
      described_class.new(address, cluster, monitoring, listeners, TEST_OPTIONS)
    end

    let(:context) do
      server.context
    end

    after do
      expect(cluster).to receive(:pool).with(server).and_return(pool)
      server.disconnect!
    end

    it 'returns a new server context' do
      expect(context.server).to eq(server)
    end
  end

  describe '#disconnect!' do

    let(:server) do
      described_class.new(address, cluster, monitoring, listeners, TEST_OPTIONS)
    end

    it 'stops the monitor instance' do
      expect(server.instance_variable_get(:@monitor)).to receive(:stop!).and_return(true)
      expect(cluster).to receive(:pool).with(server).and_return(pool)
      server.disconnect!
    end
  end

  describe '#initialize' do

    let(:server) do
      described_class.new(
        address,
        cluster,
        monitoring,
        listeners,
        TEST_OPTIONS.merge(:heartbeat_frequency => 5)
      )
    end

    after do
      expect(cluster).to receive(:pool).with(server).and_return(pool)
      server.disconnect!
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

  describe '#scan!' do

    let(:server) do
      described_class.new(address, cluster, monitoring, listeners, TEST_OPTIONS)
    end

    after do
      expect(cluster).to receive(:pool).with(server).and_return(pool)
      server.disconnect!
    end

    it 'forces a scan on the monitor' do
      expect(server.scan!).to eq(server.description)
    end
  end

  describe '#reconnect!' do

    let(:server) do
      described_class.new(address, cluster, monitoring, listeners, TEST_OPTIONS)
    end

    before do
      expect(server.monitor).to receive(:restart!).and_call_original
    end

    after do
      expect(cluster).to receive(:pool).with(server).and_return(pool)
      server.disconnect!
    end

    it 'restarts the monitor and returns true' do
      expect(server.reconnect!).to be(true)
    end
  end
end
