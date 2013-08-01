require 'spec_helper'

describe Mongo::Pool::Socket::SSL do

  let(:host) { 'localhost' }
  let(:port) { 12345 }
  let(:timeout) { 30 }

  before do
    allow_any_instance_of(
      described_class).to receive(:connect).and_call_original
    allow_any_instance_of(::Socket).to receive(:connect) { 0 }
    allow_any_instance_of(OpenSSL::SSL::SSLSocket).to receive(:connect) { 0 }
    allow(File).to receive(:open) { double(File) }
  end

  describe '#initialize' do
    it 'responds to invocation with host, port and timeout' do
      expect(described_class).to receive(:new).with(host, port, timeout)
      described_class.new(host, port, timeout)
    end

    it 'responds to invocation with host, port, timeout and opts' do
      expect(described_class).to receive(:new).with(host, port, timeout, {})
      described_class.new(host, port, timeout, {})
    end

    it 'does not connect automatically by default' do
      expect(described_class).to_not receive(:connect)
      described_class.new(host, port, timeout)
    end

    context 'when options are provided' do

      context 'when :connect => true' do
        let(:opts) { { :connect => true } }

        it 'invokes connect automatically' do
          sock = described_class.new(host, port, timeout, opts)
          expect(sock).to have_received(:connect)
        end
      end

      context 'when :connect => false' do
        let(:opts) { { :connect => false } }
        it 'does not invoke connect automatically' do
          expect(described_class).to_not receive(:connect)
          described_class.new(host, port, timeout, opts)
        end
      end

      context 'when :connect is nil' do
        it 'invokes connect automatically' do
          sock = described_class.new(host, port, timeout)
          expect(sock).to have_received(:connect)
        end
      end

      context 'when :ssl_cert is not nil' do

        let(:opts) { { :ssl_cert => '/path/to/cert' } }

        it 'creates an certificate instance' do
          expect(OpenSSL::X509::Certificate).to receive(:new)
          described_class.new(host, port, timeout, opts)
        end

      end

      context 'when :ssl_key is not nil' do

        let(:opts) { { :ssl_key => '/path/to/key' } }

        it 'creates an rsa keyfile instance' do
          expect(OpenSSL::PKey::RSA).to receive(:new)
          described_class.new(host, port, timeout, opts)
        end

      end

      context 'when :ssl_verify is true' do

        let(:opts) { { :ssl_verify => true, :connect => false } }

        it 'enables ssl certificate verification' do
          ssl_socket = described_class.new(host, port, timeout, opts)
          ssl_verify = ssl_socket.instance_variable_get(:@ssl_verify)
          expect(ssl_verify).to be_true
        end

        it 'sets SSL context the verify mode' do
          ssl_socket = described_class.new(host, port, timeout, opts)
          ssl_context = ssl_socket.instance_variable_get(:@context)
          expect(ssl_context.verify_mode).to eq(OpenSSL::SSL::VERIFY_PEER)
        end

      end

      context 'when :ssl_ca_cert is not nil' do

        let(:opts) do
          { :ssl_ca_cert => '/path/to/ca/file', :connect => false }
        end

        it 'implies :ssl_verify => true' do
          ssl_socket = described_class.new(host, port, timeout, opts)
          ssl_verify = ssl_socket.instance_variable_get(:@ssl_verify)
          expect(ssl_verify).to be_true
        end

      end

    end

  end

  describe '#connect' do

    let(:ssl_socket) { described_class.new(host, port, 0.1) }

    it 'raises a Mongo::SocketTimeoutError on timeout' do
      allow(ssl_socket).to receive(:handle_connect) { sleep 0.2 }
      expect { ssl_socket.connect }.to raise_error(Mongo::SocketTimeoutError)
    end

    it 're-raises exception after unsuccessful connect attempt' do
      allow_any_instance_of(::Socket).to receive(:connect) { raise IOError }
      expect { ssl_socket.connect }.to raise_error(IOError)
    end

    context 'when peer cert verification is requested' do

      let(:opts) { { :ssl_verify => true } }
      let(:ssl_socket) { described_class.new(host, port, timeout, opts) }

      it 'verifies the peer certificate identity' do
        allow(OpenSSL::SSL).to receive(:verify_certificate_identity) { true }
        expect(OpenSSL::SSL).to receive(:verify_certificate_identity)
        ssl_socket.connect
      end

      it 'raises Mongo::Socket error if verification fails' do
        allow(OpenSSL::SSL).to receive(:verify_certificate_identity) { false }
        expect { ssl_socket.connect }.to raise_error(Mongo::SocketError)
      end

    end

  end

  let(:socket) { double(::Socket) }
  let(:object) { described_class.new(host, port, timeout) }

  include_examples 'shared socket behavior'

end
