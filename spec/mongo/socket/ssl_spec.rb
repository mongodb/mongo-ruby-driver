require 'spec_helper'

describe Mongo::Socket::SSL, if: running_ssl? do

  let(:socket) do
    described_class.new(*default_address.to_s.split(":"), default_address.host, 5, Socket::PF_INET, options)
  end

  let(:options) do
    {
      :ssl => true,
      :ssl_cert => CLIENT_CERT_PEM,
      :ssl_key => CLIENT_KEY_PEM,
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

    context 'when a certificate and key are provided as strings' do

      let(:options) do
        key = File.read(CLIENT_KEY_PEM)
        cert = File.read(CLIENT_CERT_PEM)
        super().merge({
          :ssl_cert_string => cert,
          :ssl_key_string => key,
          :ssl_cert => nil,
          :ssl_key => nil
        })
      end

      before do
        socket.connect!
      end

      it 'connects to the server' do
        expect(socket).to be_alive
      end

    end

    context 'when certificate and an encrypted key are provided as strings' do

      let(:options) do
        key = File.read(CLIENT_KEY_ENCRYPTED_PEM)
        cert = File.read(CLIENT_CERT_PEM)
        super().merge({
          :ssl_cert_string => cert,
          :ssl_key_string => key,
          :ssl_cert => nil,
          :ssl_key => nil,
          :ssl_key_pass_phrase => CLIENT_KEY_PASSPHRASE
        })
      end

      before do
        socket.connect!
      end

      it 'connects to the server' do
        expect(socket).to be_alive
      end

    end

    context 'when a certificate is specified using both file and PEM-encoded string' do

      let(:options) do
        cert_string = "This is an invalid cert string"
        super().merge({
          :ssl_cert_string => cert_string
        })
      end

      before do
        socket.connect!
      end

      # since the other option is clearly invalid we verify priority by checking that it connects
      it 'respects the options priority' do
        expect(socket).to be_alive
      end

    end

    context 'when a certificate and key are provided as objects' do

      let(:options) do
        key = OpenSSL::PKey.read(File.open(CLIENT_KEY_PEM))
        cert = OpenSSL::X509::Certificate.new(File.read(CLIENT_CERT_PEM))
        super().merge({
          :ssl_cert_object => cert,
          :ssl_key_object => key,
          :ssl_cert => nil,
          :ssl_key => nil
        })
      end

      before do
        socket.connect!
      end

      it 'connects to the server' do
        expect(socket).to be_alive
      end

    end

    context 'when a certificate is passed by it is not of the right type' do

      let(:options) do
        key = OpenSSL::PKey.read(File.open(CLIENT_KEY_PEM))
        cert = "This is a string, not a X509 Certificate"
        super().merge({
          :ssl_cert_object => cert,
          :ssl_key_object => key,
          :ssl_cert => nil,
          :ssl_key => nil
        })
      end


      it 'raise a TypeError' do
        expect{
          socket.connect!
        }.to raise_exception(TypeError)
      end
    end

    context 'when a key is passed by it is not of the right type' do
      let(:options) do
        key = "This is a string not a key"
        super().merge({
          :ssl_key_object => key,
          :ssl_key => nil
        })
      end


      it 'raise a TypeError' do
        expect{
          socket.connect!
        }.to raise_exception(TypeError)
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
        }.to raise_exception(ArgumentError)
      end
    end

    context 'when a CA certificate is provided', if: testing_ssl_locally? do

      context 'as a path to a file' do

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

      context 'as a string containg the PEM-encoded certificate' do

        let (:options) do
          cert = File.read(CA_PEM)
          super().merge({
            :ssl_ca_cert_string => cert,
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

      context 'as an array of Certificate objects' do
        let (:options) do
          cert = [OpenSSL::X509::Certificate.new(File.read(CA_PEM))]
          super().merge({
            :ssl_ca_cert_object => cert,
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
