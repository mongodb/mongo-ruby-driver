require 'spec_helper'

describe Mongo::Pool::Socket do

  describe '.create' do

    context 'when the port is nil' do

      let(:socket) do
        described_class.create('/path/to/socket.sock', nil, 5)
      end

      it 'creates a unix socket' do
        expect(socket).to be_a(Mongo::Pool::Socket::Unix)
      end
    end

    context 'when ssl options exist' do

      let(:socket) do
        described_class.create('127.0.0.1', 27017, 5, :ssl => true)
      end

      it 'creates an ssl socket' do
        expect(socket).to be_a(Mongo::Pool::Socket::SSL)
      end
    end

    context 'when a port and no ssl options exist' do

      let(:socket) do
        described_class.create('127.0.0.1', 27017, 5)
      end

      it 'creates a tcp socket' do
        expect(socket).to be_a(Mongo::Pool::Socket::TCP)
      end
    end
  end
end
