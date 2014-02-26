require 'spec_helper'

describe Mongo::Pool::Socket::SSL do

  describe '#connect!' do

    let(:wrapper) do
      double('ssl_socket')
    end

    let(:sock) do
      double('socket')
    end

    before do
      expect(socket).to receive(:handle_connect).and_return(sock)
      expect(OpenSSL::SSL::SSLSocket).to receive(:new).with(sock, socket.context).and_return(wrapper)
      expect(wrapper).to receive(:sync_close=).with(true)
      expect(wrapper).to receive(:connect)
    end

    let(:connected) do
      socket.connect!
    end

    context 'when verifying the certificate' do

      let(:socket) do
        described_class.new('127.0.0.1', 27017, 5, Socket::PF_INET, :ssl_verify => true)
      end

      before do
        expect(wrapper).to receive(:peer_cert).and_return(nil)
        expect(OpenSSL::SSL).to receive(:verify_certificate_identity).with(nil, '127.0.0.1').and_return(true)
      end

      it 'connects the socket using ssl' do
        expect(connected).to eq(socket)
      end
    end

    context 'when not verifying the certificate' do

      let(:socket) do
        described_class.new('127.0.0.1', 27017, 5, Socket::PF_INET, :ssl => true)
      end

      it 'connects the socket using ssl' do
        expect(connected).to eq(socket)
      end
    end
  end

  describe '#initialize' do

    let(:socket) do
      described_class.new('127.0.0.1', 27017, 10, Socket::PF_INET)
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

    it 'sets the ssl context' do
      expect(socket.context).to be_a(OpenSSL::SSL::SSLContext)
    end
  end

  describe '#verifying_certificate?' do

    context 'when the verify mode is not nil' do

      let(:socket) do
        described_class.new('127.0.0.1', 27017, 10, Socket::PF_INET, :ssl_verify => true)
      end

      it 'returns true' do
        expect(socket).to be_verifying_certificate
      end
    end

    context 'when the verify mode is nil' do

      let(:socket) do
        described_class.new('127.0.0.1', 27017, 10, Socket::PF_INET)
      end

      it 'returns false' do
        expect(socket).to_not be_verifying_certificate
      end
    end
  end
end
