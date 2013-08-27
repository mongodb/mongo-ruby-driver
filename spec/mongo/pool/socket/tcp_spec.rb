require 'spec_helper'

describe Mongo::Pool::Socket::TCP do

  let(:host) { 'localhost' }
  let(:port) { 12345 }
  let(:timeout) { 30 }

  before do
    allow_any_instance_of(
      described_class).to receive(:connect).and_call_original
    allow_any_instance_of(::Socket).to receive(:connect) { 0 }
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
    end

  end

  describe '#connect' do

    let(:tcp_socket) { described_class.new(host, port, 0.1) }

    it 'invokes handle_connect with no args' do
      expect(tcp_socket).to receive(:handle_connect).with(no_args)
      tcp_socket.connect
    end

    it 'raises a Mongo::SocketTimeoutError on timeout' do
      allow(tcp_socket).to receive(:handle_connect) { sleep 0.2 }
      expect { tcp_socket.connect }.to raise_error
    end

    it 're-raises exception after unsuccessful connect attempt' do
      allow_any_instance_of(::Socket).to receive(:connect) { raise IOError }
      expect { tcp_socket.connect }.to raise_error(IOError)
    end

  end

  let(:socket) { double(::Socket) }
  let(:object) { described_class.new(host, port, timeout) }

  include_examples 'shared socket behavior'

end
