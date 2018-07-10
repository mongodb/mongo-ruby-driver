require 'lite_spec_helper'

describe Mongo::Socket do

  let(:socket) do
    described_class.new(Socket::PF_INET)
  end

  describe '#handle_errors' do
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
          raise SystemCallError.new('Test error', Errno::ENOMEDIUM::Errno)
        end
      end.to raise_error(Mongo::Error::SocketError, 'Errno::ENOMEDIUM: No medium found - Test error')
    end

    it 'maps IOError and preserves message' do
      expect do
        socket.send(:handle_errors) do
          raise IOError.new('Test error')
        end
      end.to raise_error(Mongo::Error::SocketError, 'IOError: Test error')
    end

    it 'maps SSLError and preserves message' do
      expect do
        socket.send(:handle_errors) do
          raise OpenSSL::SSL::SSLError.new('Test error')
        end
      end.to raise_error(Mongo::Error::SocketError, 'OpenSSL::SSL::SSLError: Test error (MongoDB may not be configured with SSL support)')
    end
  end
end
