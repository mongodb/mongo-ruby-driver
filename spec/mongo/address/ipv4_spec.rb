require 'spec_helper'

describe Mongo::Address::IPv4 do

  describe '#initialize' do

    context 'when a port is provided' do

      let(:resolver) do
        described_class.new('127.0.0.1:27017')
      end

      it 'sets the port' do
        expect(resolver.port).to eq(27017)
      end

      it 'sets the host' do
        expect(resolver.host).to eq('127.0.0.1')
      end
    end

    context 'when no port is provided' do

      let(:resolver) do
        described_class.new('127.0.0.1')
      end

      it 'sets the port to 27017' do
        expect(resolver.port).to eq(27017)
      end

      it 'sets the host' do
        expect(resolver.host).to eq('127.0.0.1')
      end
    end
  end

  describe '#socket' do

    let(:resolver) do
      described_class.new('127.0.0.1')
    end

    context 'when ssl options are provided' do

      let(:socket) do
        resolver.socket(5, :ssl => true)
      end

      it 'returns an ssl socket' do
        expect(socket).to be_a(Mongo::Socket::SSL)
      end

      it 'sets the family as ipv4' do
        expect(socket.family).to eq(Socket::PF_INET)
      end
    end

    context 'when ssl options are not provided' do

      let(:socket) do
        resolver.socket(5)
      end

      it 'returns a tcp socket' do
        expect(socket).to be_a(Mongo::Socket::TCP)
      end

      it 'sets the family a ipv4' do
        expect(socket.family).to eq(Socket::PF_INET)
      end
    end
  end
end
