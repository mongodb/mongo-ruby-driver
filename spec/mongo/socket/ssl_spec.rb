require 'spec_helper'

describe Mongo::Socket::SSL, if: running_ssl? do

  let(:socket) do
    described_class.new(*DEFAULT_ADDRESS.split(":"), DEFAULT_ADDRESS.split(":")[0], 5, Socket::PF_INET, options)
  end

  let(:options) do
    {
      :ssl => true,
      :ssl_cert => CLIENT_PEM,
      :ssl_key => CLIENT_PEM,
      :ssl_verify => false
    }
  end

  describe '#connect!' do

    context 'when a certificate is provided' do

      context 'when connecting the tcp socket is successful' do

        before do
          socket.connect!
        end

        it 'connects to the server' do
          expect(socket).to be_alive
        end
      end

      context 'when connecting the tcp socket raises an exception' do

        before do
          tcp_socket = socket.instance_variable_get(:@tcp_socket)
          allow(tcp_socket).to receive(:connect).and_raise(Mongo::Error::SocketTimeoutError)
        end

        let!(:result) do
          begin
            socket.connect!
          rescue => e
            e
          end
        end

        it 'raises an exception' do
          expect(result).to be_a(Mongo::Error::SocketTimeoutError)
        end
      end
    end

    context 'when a bad certificate is provided' do

      let(:options) do
        super().merge({
          :ssl_key => CRL_PEM
        })
      end

      it 'raises an exception' do
        expect {
          socket.connect!
        }.to raise_exception(OpenSSL::PKey::RSAError)
      end
    end

    context 'when a CA certificate is provided', if: testing_ssl_locally? do

      let(:options) do
        super().merge({
          :ssl_ca_cert => CA_PEM,
          :ssl_verify => true
        })
      end

      before do
        socket.connect!
      end

      it 'connects to the server' do
        expect(socket).to be_alive
      end
    end

    context 'when a CA certificate is not provided', if: testing_ssl_locally? do

      let(:options) do
        super().merge({
          :ssl_verify => true
        })
      end

      before do
        ENV['SSL_CERT_FILE']= CA_PEM
        socket.connect!
      end

      it 'uses the default cert store' do
        expect(socket).to be_alive
      end
    end

    context 'when ssl_verify is not specified', if: testing_ssl_locally? do

      let(:options) do
        super().merge({
          :ssl_ca_cert => CA_PEM
        }).tap { |options| options.delete(:ssl_verify) }
      end

      before do
        socket.connect!
      end

      it 'verifies the server certificate' do
        expect(socket).to be_alive
      end
    end

    context 'when ssl_verify is true', if: testing_ssl_locally? do

      let(:options) do
        super().merge({
          :ssl_ca_cert => CA_PEM,
          :ssl_verify => true
        })
      end

      before do
        socket.connect!
      end

      it 'verifies the server certificate' do
        expect(socket).to be_alive
      end
    end

    context 'when ssl_verify is false' do

      let(:options) do
        super().merge({
          :ssl_ca_cert => 'invalid',
          :ssl_verify => false
        })
      end

      before do
        socket.connect!
      end

      it 'does not verify the server certificate' do
        expect(socket).to be_alive
      end
    end
  end

  describe '#readbyte' do

    before do
      allow_message_expectations_on_nil

      allow(socket.socket).to receive(:read) do |length|
        socket_content[0, length]
      end
    end

    context 'with the socket providing "abc"' do

      let(:socket_content) { "abc" }

      it 'should return 97 (the byte for "a")' do
        expect(socket.readbyte).to eq(97)
      end
    end

    context 'with the socket providing "\x00" (NULL_BYTE)' do

      let(:socket_content) { "\x00" }

      it 'should return 0' do
        expect(socket.readbyte).to eq(0)
      end
    end

    context 'with the socket providing no data' do

      let(:socket_content) { "" }

      it 'should raise EOFError' do
        expect { socket.readbyte }
          .to raise_error(Mongo::Error::SocketError).with_message("EOFError")
      end
    end
  end
end
