require 'spec_helper'

describe Mongo::Socket::SSL do
  require_ssl

  let(:address) do
    default_address
  end

  let(:resolver) do
    address.send(:create_resolver, {})
  end

  let(:socket_timeout) do
    1
  end

  let(:socket) do
    resolver.socket(socket_timeout, options)
  end

  let(:options) do
    SpecConfig.instance.ssl_options
  end

  let (:key_string) do
    File.read(SpecConfig.instance.client_key_pem)
  end

  let (:cert_string) do
    File.read(SpecConfig.instance.client_cert_pem)
  end

  let (:ca_cert_string) do
    File.read(CA_PEM)
  end

  let(:key_encrypted_string) do
    File.read(CLIENT_KEY_ENCRYPTED_PEM)
  end

  let(:cert_object) do
    OpenSSL::X509::Certificate.new(cert_string)
  end

  let(:key_object) do
    OpenSSL::PKey.read(key_string)
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
        {
          :ssl => true,
          :ssl_cert_string => cert_string,
          :ssl_key_string => key_string,
          :ssl_verify => false
        }
      end

      before do
        socket.connect!
      end

      it 'connects to the server' do
        expect(socket).to be_alive
      end
    end

    context 'when certificate and an encrypted key are provided as strings' do
      require_local_tls

      let(:options) do
        {
          :ssl => true,
          :ssl_cert_string => cert_string,
          :ssl_key_string => key_encrypted_string,
          :ssl_key_pass_phrase => CLIENT_KEY_PASSPHRASE,
          :ssl_verify => false
        }
      end

      before do
        socket.connect!
      end

      it 'connects to the server' do
        expect(socket).to be_alive
      end
    end

    context 'when a certificate and key are provided as objects' do

      let(:options) do
        {
          :ssl => true,
          :ssl_cert_object => cert_object,
          :ssl_key_object => key_object,
          :ssl_verify => false
        }
      end

      before do
        socket.connect!
      end

      it 'connects to the server' do
        expect(socket).to be_alive
      end
    end

    context 'when the certificate is specified using both a file and a PEM-encoded string' do

      let(:options) do
        super().merge(
          :ssl_cert_string => 'This is a random string, not a PEM-encoded certificate'
        )
      end

      before do
        socket.connect!
      end

      # since the lower priority option is clearly invalid we verify priority by checking that it connects
      it 'discards the value of :ssl_cert_string' do
        expect(socket).to be_alive
      end
    end

    context 'when the certificate is specified using both a file and an object' do

      let(:options) do
        super().merge(
          :ssl_cert_object => 'This is a string, not a certificate'
        )
      end

      before do
        socket.connect!
      end

      # since the lower priority option is clearly invalid we verify priority by checking that it connects
      it 'discards the value of :ssl_cert_object' do
        expect(socket).to be_alive
      end
    end

    context 'when the certificate is specified using both a PEM-encoded string and an object' do

      let(:options) do
        {
          :ssl => true,
          :ssl_cert_string => cert_string,
          :ssl_cert_object => 'This is a string, not a Certificate',
          :ssl_key => CLIENT_KEY_PEM,
          :ssl_verify => false
        }
      end

      before do
        socket.connect!
      end

      # since the lower priority option is clearly invalid we verify priority by checking that it connects
      it 'discards the value of :ssl_cert_object' do
        expect(socket).to be_alive
      end
    end

    context 'when the key is specified using both a file and a PEM-encoded string' do

      let(:options) do
        super().merge(
          :ssl_key_string => 'This is a normal string, not a PEM-encoded key'
        )
      end

      before do
        socket.connect!
      end

      # since the lower priority option is clearly invalid we verify priority by checking that it connects
      it 'discards the value of :ssl_key_string' do
        expect(socket).to be_alive
      end
    end

    context 'when the key is specified using both a file and an object' do

      let(:options) do
        super().merge(
          :ssl_cert_object => 'This is a string, not a key'
        )
      end

      before do
        socket.connect!
      end

      # since the lower priority option is clearly invalid we verify priority by checking that it connects
      it 'discards the value of :ssl_key_object' do
        expect(socket).to be_alive
      end
    end

    context 'when the key is specified using both a PEM-encoded string and an object' do

      let(:options) do
        {
          :ssl => true,
          :ssl_cert => CLIENT_CERT_PEM,
          :ssl_key_string => key_string,
          :ssl_key_object => 'This is a string, not a PKey',
          :ssl_verify => false
        }
      end

      before do
        socket.connect!
      end

      # since the lower priority option is clearly invalid we verify priority by checking that it connects
      it 'discards the value of :ssl_key_object' do
        expect(socket).to be_alive
      end
    end

    context 'when a certificate is passed, but it is not of the right type' do

      let(:options) do
        cert = "This is a string, not a X509 Certificate"
        {
          :ssl => true,
          :ssl_cert_object => cert,
          :ssl_key => CLIENT_KEY_PEM,
          :ssl_verify => false
        }
      end

      it 'raises a TypeError' do
        expect{
          socket.connect!
        }.to raise_exception(TypeError)
      end
    end

    context 'when the hostname is incorrect' do
      let(:host_name) do
        'incorrect_hostname'
      end

      context 'when the hostname is verified' do
        it 'raises an error' do
          error = nil
          begin
            described_class.new(
              address.host,
              address.port,
              host_name,
              30,
              ::Socket::PF_INET,
              options.merge(ssl_verify_hostname: true)
            ).connect!
          rescue => e
            error = e
          end

          expect(error).to be_a(Mongo::Error::SocketError)
          expect(error.message).to eq('SSL handshake failed due to a hostname mismatch.')
        end
      end

      context 'when the hostname is not verified' do
        it 'raises an error' do
          expect {
            described_class.new(
              address.host,
              address.port,
              host_name,
              30,
              ::Socket::PF_INET,
              options.merge(ssl_verify_hostname: false)
            ).connect!
          }.not_to raise_error
        end
      end
    end

    context 'when ruby version is < 2.4.1' do
      ruby_version_lt '2.4.1'

      context 'when a key is passed, but it is not of the right type' do

        let(:options) do
          key = "This is a string not a key"
          {
              :ssl => true,
              :ssl_key_object => key,
              :ssl_cert => CLIENT_CERT_PEM,
              :ssl_verify => false
          }
        end

        it 'raises a TypeError' do
          expect{
            socket.connect!
          }.to raise_exception(TypeError)
        end
      end
    end

    # Note that as of MRI 2.4, Creating a socket with the wrong key type raises
    # a NoMethodError because #private? is attempted to be called on the key.
    # In jruby 9.2 a TypeError is raised.
    # In jruby 9.1 a OpenSSL::PKey::PKeyError is raised.
    context 'when ruby version is >= 2.4.1' do
      ruby_version_gte '2.4.1'

      context 'when a key is passed, but it is not of the right type' do

        let(:options) do
          key = "This is a string not a key"
          {
              :ssl => true,
              :ssl_key_object => key,
              :ssl_cert => CLIENT_CERT_PEM,
              :ssl_verify => false
          }
        end

        let(:expected_exception) do
          if SpecConfig.instance.jruby?
            if RUBY_VERSION >= '2.5.0'
              # jruby 9.2
              TypeError
            else
              # jruby 9.1
              OpenSSL::OpenSSLError
            end
          else
            NoMethodError
          end
        end

        it 'raises a NoMethodError' do
          expect{
            socket.connect!
          }.to raise_exception(expected_exception)
        end
      end
    end

    context 'when a bad certificate is provided' do

      let(:options) do
        super().merge(
          :ssl_key => COMMAND_MONITORING_TESTS.first
        )
      end

      let(:expected_exception) do
        if SpecConfig.instance.jruby?
          # java.lang.ClassCastException: org.bouncycastle.asn1.DERApplicationSpecific cannot be cast to org.bouncycastle.asn1.ASN1Sequence
          # https://github.com/jruby/jruby-openssl/issues/171
          Exception
        else
          # mri
          if RUBY_VERSION >= '2.4.0'
            # OpenSSL::PKey::PKeyError: Could not parse PKey: no start line
            OpenSSL::OpenSSLError
          else
            # ArgumentError: Could not parse PKey: no start line
            ArgumentError
          end
        end
      end

      it 'raises an exception' do
        expect {
          socket.connect!
        }.to raise_exception(expected_exception)
      end
    end

    context 'when a CA certificate is provided' do
      require_local_tls

      context 'as a path to a file' do

        let(:options) do
          super().merge(
            :ssl_ca_cert => CA_PEM,
            :ssl_verify => true
          )
        end

        before do
          socket.connect!
        end

        it 'connects to the server' do
          expect(socket).to be_alive
        end
      end

      context 'as a string containing the PEM-encoded certificate' do

        let (:options) do
          super().merge(
            :ssl_ca_cert_string => ca_cert_string,
            :ssl_verify => true
          )
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
          cert = [OpenSSL::X509::Certificate.new(ca_cert_string)]
          super().merge(
            :ssl_ca_cert_object => cert,
            :ssl_verify => true
          )
        end

        before do
          socket.connect!
        end

        it 'connects to the server' do
          expect(socket).to be_alive
        end
      end

      context 'both as a file and a PEM-encoded parameter' do

        let(:options) do
          super().merge(
            :ssl_ca_cert => CA_PEM,
            :ssl_ca_cert_string => 'This is a string, not a certificate',
            :ssl_verify => true
          )
        end

        before do
          socket.connect!
        end

        # since the lower priority option is clearly invalid we verify priority by checking that it connects
        it 'discards the value of :ssl_ca_cert_string' do
          expect(socket).to be_alive
        end
      end

      context 'both as a file and as object parameter' do

        let(:options) do
          super().merge(
            :ssl_ca_cert => CA_PEM,
            :ssl_ca_cert_object => 'This is a string, not an array of certificates',
            :ssl_verify => true
          )
        end

        before do
          socket.connect!
        end

        it 'discards the value of :ssl_ca_cert_object' do
          expect(socket).to be_alive
        end
      end

      context 'both as a PEM-encoded string and as object parameter' do

        let(:options) do
          cert = File.read(CA_PEM)
          super().merge(
            :ssl_ca_cert_string => cert,
            :ssl_ca_cert_object => 'This is a string, not an array of certificates',
            :ssl_verify => true
          )
        end

        before do
          socket.connect!
        end

        it 'discards the value of :ssl_ca_cert_object' do
          expect(socket).to be_alive
        end
      end
    end

    context 'when a CA certificate is not provided' do
      require_local_tls

      let(:options) do
        super().merge(
          :ssl_verify => true
        )
      end

      before do
        ENV['SSL_CERT_FILE'] = CA_PEM
        socket.connect!
      end

      it 'uses the default cert store' do
        expect(socket).to be_alive
      end
    end

    context 'when ssl_verify is not specified' do
      require_local_tls

      let(:options) do
        super().merge(
          :ssl_ca_cert => CA_PEM
        ).tap { |options| options.delete(:ssl_verify) }
      end

      before do
        socket.connect!
      end

      it 'verifies the server certificate' do
        expect(socket).to be_alive
      end
    end

    context 'when ssl_verify is true' do
      require_local_tls

      let(:options) do
        super().merge(
          :ssl_ca_cert => CA_PEM,
          :ssl_verify => true
        )
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
        super().merge(
          :ssl_ca_cert => 'invalid',
          :ssl_verify => false
        )
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
          .to raise_error(Mongo::Error::SocketError).with_message("EOFError: EOFError")
      end
    end
  end
end
