require 'spec_helper'

describe Mongo::Server do

  describe '#alive?' do

    let(:address) do
      '127.0.0.1:27017'
    end

    let(:server) do
      described_class.new(address)
    end

    context 'when the server has been refreshed' do

      context 'when the server is alive' do

        before do
          server.instance_variable_set(:@alive, true)
        end

        it 'returns true' do
          expect(server).to be_alive
        end
      end

      context 'when the server is not alive' do

        before do
          server.instance_variable_set(:@alive, false)
        end

        it 'returns false' do
          expect(server).to_not be_alive
        end
      end
    end

    context 'when the server has not been refreshed' do

      it 'returns false' do
        expect(server).to_not be_alive
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

    it 'sets the address' do
      expect(server.address).to eq(address)
    end

    it 'sets the options' do
      expect(server.options).to eq(:refresh_interval => 5)
    end
  end

  describe '#refresh' do

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
