require 'spec_helper'

describe Mongo::Socket::TCP do

  let(:socket) do
    described_class.new('127.0.0.1', 27017, 30, Socket::PF_INET)
  end

  describe '#connect!' do

    before do
      socket.connect!
    end

    after do
      socket.close
    end

    it 'connects to the server' do
      expect(socket).to be_alive
    end
  end

  describe '#alive?' do

    context 'when the socket is connected' do

      before do
        socket.connect!
      end

      after do
        socket.close
      end

      it 'returns true' do
        expect(socket).to be_alive
      end
    end

    context 'when the socket is not connected' do

      before do
        socket.close
      end

      it 'returns false' do
        expect(socket).to_not be_alive
      end
    end
  end
end
