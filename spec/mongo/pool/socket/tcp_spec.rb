require 'spec_helper'

describe Mongo::Pool::Socket::TCP do

  describe '#connect' do

    let(:socket) do
      described_class.new('127.0.0.1', 27017, 10)
    end

    let(:connected) do
      socket.connect!
    end

    it 'connects the socket over tcp' do
      expect(connected).to eq(socket)
    end

    it 'returns an alive socket' do
      expect(connected).to be_alive
    end
  end

  describe '#initialize' do

    let(:socket) do
      described_class.new('127.0.0.1', 27017, 10)
    end

    it 'sets the host' do
      expect(socket.host).to eq('127.0.0.1')
    end

    it 'sets the port' do
      expect(socket.port).to eq(27017)
    end

    it 'sets the timeout' do
      expect(socket.timeout).to eq(10)
    end
  end
end
