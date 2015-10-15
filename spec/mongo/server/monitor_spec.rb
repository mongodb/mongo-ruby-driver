require 'spec_helper'

describe Mongo::Server::Monitor do

  let(:address) do
    Mongo::Address.new(DEFAULT_ADDRESS)
  end

  let(:listeners) do
    Mongo::Event::Listeners.new
  end

  describe '#scan!' do

    context 'when calling multiple times in succession' do

      let(:monitor) do
        described_class.new(address, listeners, TEST_OPTIONS)
      end

      it 'throttles the scans to minimum 500ms' do
        start = Time.now
        monitor.scan!
        monitor.scan!
        expect(Time.now - start).to be >= 0.5
      end
    end

    context 'when the ismaster command succeeds' do

      let(:monitor) do
        described_class.new(address, listeners, TEST_OPTIONS)
      end

      before do
        monitor.scan!
      end

      it 'updates the server description', if: standalone? do
        expect(monitor.description).to be_standalone
      end

      it 'updates the server description', if: replica_set? do
        expect(monitor.description).to be_primary
      end

      it 'updates the server description', if: sharded? do
        expect(monitor.description).to be_mongos
      end
    end

    context 'when the ismaster command fails' do

      context 'when no server is running on the address' do

        let(:bad_address) do
          Mongo::Address.new('127.0.0.1:27050')
        end

        let(:monitor) do
          described_class.new(bad_address, listeners)
        end

        before do
          monitor.scan!
        end

        it 'keeps the server unknown' do
          expect(monitor.description).to be_unknown
        end
      end

      context 'when the socket gets an exception' do

        let(:bad_address) do
          Mongo::Address.new(DEFAULT_ADDRESS)
        end

        let(:monitor) do
          described_class.new(bad_address, listeners)
        end

        let(:socket) do
          monitor.connection.connect!
          monitor.connection.__send__(:socket)
        end

        before do
          expect(socket).to receive(:write).and_raise(Mongo::Error::SocketError)
          monitor.scan!
        end

        it 'keeps the server unknown' do
          expect(monitor.description).to be_unknown
        end

        it 'disconnects the connection' do
          expect(monitor.connection).to_not be_connected
        end
      end
    end
  end

  describe '#heartbeat_frequency' do

    context 'when an option is provided' do

      let(:monitor) do
        described_class.new(address, listeners, :heartbeat_frequency => 5)
      end

      it 'returns the option' do
        expect(monitor.heartbeat_frequency).to eq(5)
      end
    end

    context 'when no option is provided' do

      let(:monitor) do
        described_class.new(address, listeners)
      end

      it 'defaults to 5' do
        expect(monitor.heartbeat_frequency).to eq(10)
      end
    end
  end

  describe '#run!' do

    let(:monitor) do
      described_class.new(address, listeners, :heartbeat_frequency => 1)
    end

    before do
      monitor.run!
      sleep(1)
    end

    it 'refreshes the server on the provided interval' do
      expect(monitor.description).to_not be_nil
    end
  end

  describe '#restart!' do

    let(:monitor) do
      described_class.new(address, listeners, TEST_OPTIONS)
    end

    let!(:thread) do
      monitor.run!
    end

    context 'when the monitor is already running' do

      it 'does not create a new thread' do
        expect(monitor.restart!).to be(thread)
      end
    end

    context 'when the monitor is not already running' do

      before do
        monitor.stop!
        sleep(1)
      end

      it 'creates a new thread' do
        expect(monitor.restart!).not_to be(thread)
      end
    end
  end

  describe '#stop' do

    let(:monitor) do
      described_class.new(address, listeners, TEST_OPTIONS)
    end

    let!(:thread) do
      monitor.run!
    end

    before do
      expect(monitor.connection).to receive(:disconnect!).and_call_original
      monitor.stop!
      sleep(1)
    end

    it 'kills the monitor thread' do
      expect(thread.stop?).to be(true)
    end
  end

  describe '#connection' do

    context 'when there is a connect_timeout option set' do

      let(:connect_timeout) do
        1
      end

      let(:monitor) do
        described_class.new(address, listeners, TEST_OPTIONS.merge(connect_timeout: connect_timeout))
      end

      it 'sets the value as the timeout on the connection' do
        expect(monitor.connection.timeout).to eq(connect_timeout)
      end

      it 'set the value as the timeout on the socket' do
        monitor.connection.connect!
        expect(monitor.connection.send(:socket).timeout).to eq(connect_timeout)
      end
    end
  end
end
