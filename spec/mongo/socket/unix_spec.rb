require 'spec_helper'

describe Mongo::Socket::Unix do

  let(:socket) do
    described_class.new("/tmp/mongodb-27017.sock", 5)
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

      it 'raises error' do
        expect(socket).to_not be_alive
      end
    end
  end
end
