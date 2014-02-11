require 'spec_helper'

describe Mongo::Server::Address::Sock do

  describe '#initialize' do

    let(:resolver) do
      described_class.new('/path/to/socket.sock')
    end

    it 'sets the ip to nil' do
      expect(resolver.ip).to be_nil
    end

    it 'sets the port to nil' do
      expect(resolver.port).to be_nil
    end

    it 'sets the host' do
      expect(resolver.host).to eq('/path/to/socket.sock')
    end
  end
end
