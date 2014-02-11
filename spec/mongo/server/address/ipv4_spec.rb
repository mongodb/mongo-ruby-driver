require 'spec_helper'

describe Mongo::Server::Address::IPv4 do

  describe '#initialize' do

    context 'when a port is provided' do

      let(:resolver) do
        described_class.new('127.0.0.1:27017')
      end

      it 'sets the host ip' do
        expect(resolver.ip).to eq('127.0.0.1')
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

      it 'sets the host ip' do
        expect(resolver.ip).to eq('127.0.0.1')
      end

      it 'sets the port to 27017' do
        expect(resolver.port).to eq(27017)
      end

      it 'sets the host' do
        expect(resolver.host).to eq('127.0.0.1')
      end
    end
  end
end
