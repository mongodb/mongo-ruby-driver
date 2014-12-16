require 'spec_helper'

describe Mongo::Server::Address::Unix do

  describe '#initialize' do

    let(:resolver) do
      described_class.new('/path/to/socket.sock')
    end

    it 'sets the host' do
      expect(resolver.host).to eq('/path/to/socket.sock')
    end
  end

  describe '#socket' do

    let(:resolver) do
      described_class.new('/path/to/socket.sock')
    end

    let(:socket) do
      resolver.socket(5)
    end

    it 'returns a unix socket' do
      expect(socket).to be_a(Mongo::Socket::Unix)
    end
  end
end
