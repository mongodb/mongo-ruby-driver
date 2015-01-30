require 'spec_helper'

describe Mongo::Server::Monitor do

  let(:address) do
    Mongo::Address.new('127.0.0.1:27017')
  end

  let(:listeners) do
    Mongo::Event::Listeners.new
  end

  describe '#scan!' do

    context 'when the ismaster command succeeds' do

      let(:monitor) do
        described_class.new(address, listeners)
      end

      before do
        monitor.scan!
      end

      it 'updates the server description' do
        expect(monitor.description).to be_standalone
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
          Mongo::Address.new('127.0.0.1:27017')
        end

        let(:monitor) do
          described_class.new(bad_address, listeners)
        end

        let(:socket) do
          monitor.connection.connect!
          monitor.connection.__send__(:socket)
        end

        before do
          expect(socket).to receive(:write).and_raise(SocketError)
          monitor.scan!
        end

        it 'keeps the server unknown' do
          expect(monitor.description).to be_unknown
        end

        it 'disconnects the connection' do
          expect(monitor.connection.send(:socket)).to be_nil
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
end
