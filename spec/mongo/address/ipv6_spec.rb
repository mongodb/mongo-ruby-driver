require 'spec_helper'

describe Mongo::Address::IPv6 do

  let(:resolver) do
    described_class.new(*described_class.parse(address))
  end

  describe 'self.parse' do

    context 'when a port is provided' do

      it 'returns the host and port' do
        expect(described_class.parse('[::1]:27017')).to eq(['::1', 27017])
      end
    end

    context 'when no port is provided' do

      it 'returns the host and port' do
        expect(described_class.parse('[::1]')).to eq(['::1', 27017])
      end
    end
  end

  describe '#initialize' do

    context 'when a port is provided' do

      let(:address) do
        '[::1]:27017'
      end

      it 'sets the port' do
        expect(resolver.port).to eq(27017)
      end

      it 'sets the host' do
        expect(resolver.host).to eq('::1')
      end
    end

    context 'when no port is provided' do

      let(:address) do
        '[::1]'
      end

      it 'sets the port to 27017' do
        expect(resolver.port).to eq(27017)
      end

      it 'sets the host' do
        expect(resolver.host).to eq('::1')
      end
    end
  end

  describe '#socket' do

    let(:address) do
      '[::1]'
    end

    context 'when ssl options are provided' do

      let(:socket) do
        resolver.socket(5, :ssl => true)
      end

      it 'returns an ssl socket' do
        expect(socket).to be_a(Mongo::Socket::SSL)
      end

      it 'sets the family as ipv6' do
        expect(socket.family).to eq(Socket::PF_INET6)
      end
    end

    context 'when ssl options are not provided' do

      let(:socket) do
        resolver.socket(5)
      end

      it 'returns a tcp socket' do
        expect(socket).to be_a(Mongo::Socket::TCP)
      end

      it 'sets the family a ipv6' do
        expect(socket.family).to eq(Socket::PF_INET6)
      end
    end
  end
end
