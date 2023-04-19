# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Socket do

  let(:socket) do
    described_class.new(0, {})
  end

  describe '#human_address' do
    it 'raises NotImplementedError' do
      expect do
        socket.send(:human_address)
      end.to raise_error(NotImplementedError)
    end
  end

  describe '#map_exceptions' do
    before do
      expect(socket).to receive(:human_address).and_return('fake-address')
    end

    it 'maps timeout exception' do
      expect do
        socket.send(:map_exceptions) do
          raise Errno::ETIMEDOUT
        end
      end.to raise_error(Mongo::Error::SocketTimeoutError)
    end

    it 'maps SystemCallError and preserves message' do
      expect do
        socket.send(:map_exceptions) do
          raise SystemCallError.new('Test error', Errno::ENFILE::Errno)
        end
      end.to raise_error(Mongo::Error::SocketError, 'Errno::ENFILE: Too many open files in system - Test error (for fake-address)')
    end

    it 'maps IOError and preserves message' do
      expect do
        socket.send(:map_exceptions) do
          raise IOError.new('Test error')
        end
      end.to raise_error(Mongo::Error::SocketError, 'IOError: Test error (for fake-address)')
    end

    it 'maps SSLError and preserves message' do
      expect do
        socket.send(:map_exceptions) do
          raise OpenSSL::SSL::SSLError.new('Test error')
        end
      end.to raise_error(Mongo::Error::SocketError, 'OpenSSL::SSL::SSLError: Test error (for fake-address)')
    end
  end

  describe '#read' do
    let(:target_host) do
      host = ClusterConfig.instance.primary_address_host
      # Take ipv4 address
      Socket.getaddrinfo(host, 0).detect { |ai| ai.first == 'AF_INET' }[3]
    end

    let(:socket) do
      Mongo::Socket::TCP.new(target_host, ClusterConfig.instance.primary_address_port, 1, Socket::PF_INET)
    end

    let(:raw_socket) { socket.instance_variable_get('@socket') }

    let(:wait_readable_class) do
      Class.new(Exception) do
        include IO::WaitReadable
      end
    end

    context 'timeout' do
      clean_slate_for_all

      shared_examples_for 'times out' do
        it 'times out' do
          expect(socket).to receive(:timeout).at_least(:once).and_return(0.2)
          # When we raise WaitWritable, the socket object is ready for
          # writing which makes the read method invoke read_nonblock many times
          expect(raw_socket).to receive(:read_nonblock).at_least(:once) do |len, buf|
            sleep 0.01
            raise exception_class
          end

          expect do
            socket.read(10)
          end.to raise_error(Mongo::Error::SocketTimeoutError, /Took more than .* seconds to receive data.*\(for /)
        end
      end

      context 'with WaitReadable' do

        let(:exception_class) do
          Class.new(Exception) do
            include IO::WaitReadable
          end
        end

        it_behaves_like 'times out'
      end

      context 'with WaitWritable' do

        let(:exception_class) do
          Class.new(Exception) do
            include IO::WaitWritable
          end
        end

        it_behaves_like 'times out'
      end
    end
  end
end
