# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Address::Unix do

  let(:resolver) do
    described_class.new(*described_class.parse(address))
  end

  describe 'self.parse' do

    it 'returns the host and no port' do
      expect(described_class.parse('/path/to/socket.sock')).to eq(['/path/to/socket.sock'])
    end
  end

  describe '#initialize' do

    let(:address) do
      '/path/to/socket.sock'
    end

    it 'sets the host' do
      expect(resolver.host).to eq('/path/to/socket.sock')
    end
  end

  describe '#socket' do
    require_unix_socket

    let(:address) do
      "/tmp/mongodb-#{SpecConfig.instance.any_port}.sock"
    end

    let(:socket) do
      resolver.socket(5)
    end

    it 'returns a unix socket' do
      expect(socket).to be_a(Mongo::Socket::Unix)
    end
  end
end
