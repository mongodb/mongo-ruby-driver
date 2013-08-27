require 'spec_helper'

describe Mongo::Pool::Socket::Unix do

  let(:path) { '/path/to/socket.sock' }
  let(:timeout) { 30 }

  before do
    allow_any_instance_of(
      described_class).to receive(:connect).and_call_original
    allow_any_instance_of(::Socket).to receive(:connect) { 0 }
    allow(File).to receive(:open) { double(File) }
  end

  describe '#initialize' do
    it 'responds to invocation with host and timeout' do
      expect(described_class).to receive(:new).with(path, timeout)
      described_class.new(path, timeout)
    end

    it 'responds to invocation with path, timeout and opts' do
      expect(described_class).to receive(:new).with(path, timeout, {})
      described_class.new(path, timeout, {})
    end

    it 'does not connect automatically by default' do
      expect(described_class).to_not receive(:connect)
      described_class.new(path, timeout)
    end

    context 'when options are provided' do
      context 'when :connect => true' do
        let(:opts) { { :connect => true } }

        it 'invokes connect automatically' do
          sock = described_class.new(path, timeout, opts)
          expect(sock).to have_received(:connect)
        end
      end

      context 'when :connect => false' do
        let(:opts) { { :connect => false } }

        it 'does not invoke connect automatically' do
          expect(described_class).to_not receive(:connect)
          described_class.new(path, timeout, opts)
        end
      end
    end

  end

  describe '#connect' do

    let(:unix_socket) { described_class.new(path, 0.1) }

    it 'raises a Mongo::SocketTimeoutError on timeout' do
      allow(unix_socket).to receive(:create_socket) { sleep 0.2 }
      expect { unix_socket.connect }.to raise_error(Mongo::SocketTimeoutError)
    end

    it 're-raises exception after unsuccessful connect attempt' do
      allow_any_instance_of(::Socket).to receive(:connect) { raise IOError }
      expect { unix_socket.connect }.to raise_error(IOError)
    end

  end

  let(:socket) { double(::Socket) }
  let(:object) { described_class.new(path, timeout) }

  include_examples 'shared socket behavior'

end
