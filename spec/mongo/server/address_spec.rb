require 'spec_helper'

describe Mongo::Server::Address do

  describe '#resolve' do

    context 'when providing an ipv4 host' do

      context 'when a port is provided' do

        let(:address) do
          described_class.new('127.0.0.1:27017')
        end

        it 'sets the host ip' do
          expect(address.ip).to eq('127.0.0.1')
        end

        it 'sets the port' do
          expect(address.port).to eq(27017)
        end

        it 'sets the host' do
          expect(address.host).to eq('127.0.0.1')
        end
      end

      context 'when no port is provided' do

        let(:address) do
          described_class.new('127.0.0.1')
        end

        it 'sets the host ip' do
          expect(address.ip).to eq('127.0.0.1')
        end

        it 'sets the port to 27017' do
          expect(address.port).to eq(27017)
        end

        it 'sets the host' do
          expect(address.host).to eq('127.0.0.1')
        end
      end
    end

    context 'when providing a DNS entry' do

      context 'when a port is provided' do

        let(:address) do
          described_class.new('localhost:27017')
        end

        it 'resolves the host ip' do
          expect(address.ip).to eq('127.0.0.1')
        end

        it 'sets the port' do
          expect(address.port).to eq(27017)
        end

        it 'sets the host' do
          expect(address.host).to eq('localhost')
        end
      end

      context 'when a port is not provided' do

        let(:address) do
          described_class.new('localhost')
        end

        it 'resolves the host ip' do
          expect(address.ip).to eq('127.0.0.1')
        end

        it 'sets the port to 27017' do
          expect(address.port).to eq(27017)
        end

        it 'sets the host' do
          expect(address.host).to eq('localhost')
        end
      end
    end

    context 'when providing a socket path' do

      let(:address) do
        described_class.new('/path/to/socket.sock')
      end

      it 'sets the ip to nil' do
        expect(address.ip).to be_nil
      end

      it 'sets the port to nil' do
        expect(address.port).to be_nil
      end

      it 'sets the host' do
        expect(address.host).to eq('/path/to/socket.sock')
      end
    end
  end
end

