require 'spec_helper'

describe Mongo::Pool::Socket::Unix do

  describe '#connect!' do

    let(:socket) do
      described_class.new('/path/to/socket.sock', 10)
    end

    let(:sock) do
      double('socket')
    end

    context 'when an error occurs when connecting' do

      before do
        expect(socket).to receive(:create_socket).with(Socket::Constants::AF_UNIX).and_return(sock)
        expect(sock).to receive(:connect).with('/path/to/socket.sock').and_raise(IOError)
        expect(sock).to receive(:close)
      end

      it 're-raises the exception' do
        expect {
          socket.connect!
        }.to raise_error(IOError)
      end
    end

    context 'when no error occurs connecting' do

      before do
        expect(socket).to receive(:create_socket).with(Socket::Constants::AF_UNIX).and_return(sock)
        expect(sock).to receive(:connect)
      end

      it 'connects and returns the socket' do
        expect(socket.connect!).to eq(socket)
      end
    end
  end

  describe '#initialize' do

    let(:socket) do
      described_class.new('/path/to/socket.sock', 10)
    end

    it 'sets the host' do
      expect(socket.host).to eq('/path/to/socket.sock')
    end

    it 'sets the timeout' do
      expect(socket.timeout).to eq(10)
    end
  end
end
