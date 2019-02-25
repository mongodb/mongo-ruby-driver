require 'lite_spec_helper'

describe Mongo::Socket do

  let(:socket) do
    described_class.new(Socket::PF_INET)
  end

  describe '#address' do
    it 'raises NotImplementedError' do
      expect do
        socket.send(:address)
      end.to raise_error(NotImplementedError)
    end
  end

  describe '#handle_errors' do
    before do
      expect(socket).to receive(:address).and_return('fake-address')
    end

    it 'maps timeout exception' do
      expect do
        socket.send(:handle_errors) do
          raise Errno::ETIMEDOUT
        end
      end.to raise_error(Mongo::Error::SocketTimeoutError)
    end

    it 'maps SystemCallError and preserves message' do
      expect do
        socket.send(:handle_errors) do
          raise SystemCallError.new('Test error', Errno::ENFILE::Errno)
        end
      end.to raise_error(Mongo::Error::SocketError, 'Errno::ENFILE: Too many open files in system - Test error (for fake-address)')
    end

    it 'maps IOError and preserves message' do
      expect do
        socket.send(:handle_errors) do
          raise IOError.new('Test error')
        end
      end.to raise_error(Mongo::Error::SocketError, 'IOError: Test error (for fake-address)')
    end

    it 'maps SSLError and preserves message' do
      expect do
        socket.send(:handle_errors) do
          raise OpenSSL::SSL::SSLError.new('Test error')
        end
      end.to raise_error(Mongo::Error::SocketError, 'OpenSSL::SSL::SSLError: Test error (for fake-address) (MongoDB may not be configured with SSL support)')
    end
  end
end
