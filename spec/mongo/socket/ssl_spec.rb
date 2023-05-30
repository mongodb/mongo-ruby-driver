# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

# this test performs direct network connections without retries.
# In case of intermittent network issues, retry the entire failing test.
describe Mongo::Socket::SSL do
  retry_test
  clean_slate_for_all
  require_tls

  let(:host_name) { 'localhost' }

  let(:socket) do
    described_class.new('127.0.0.1', default_address.port,
      host_name, 1, :INET, ssl_options.merge(
        connect_timeout: 2.4))
  end

  let(:ssl_options) do
    SpecConfig.instance.ssl_options
  end

  let (:key_string) do
    File.read(SpecConfig.instance.local_client_key_path)
  end

  let (:cert_string) do
    File.read(SpecConfig.instance.local_client_cert_path)
  end

  let (:ca_cert_string) do
    File.read(SpecConfig.instance.local_ca_cert_path)
  end

  let(:key_encrypted_string) do
    File.read(SpecConfig.instance.client_encrypted_key_path)
  end

  let(:cert_object) do
    OpenSSL::X509::Certificate.new(cert_string)
  end

  let(:key_object) do
    OpenSSL::PKey.read(key_string)
  end

  describe '#human_address' do
    it 'returns the address and tls indicator' do
      addr = socket.instance_variable_get(:@tcp_socket).remote_address
      expect(socket.send(:human_address)).to eq("#{addr.ip_address}:#{addr.ip_port} (#{default_address}, TLS)")
    end
  end

  describe '#connect!' do
    context 'when TLS context hooks are provided' do
      # https://github.com/jruby/jruby-openssl/issues/221
      fails_on_jruby

      let(:proc) do
        Proc.new do |context|
          if BSON::Environment.jruby?
            context.ciphers = ["AES256-SHA256"]
          else
            context.ciphers = ["AES256-SHA"]
          end
        end
      end

      before do
        Mongo.tls_context_hooks = [ proc ]
      end

      after do
        Mongo.tls_context_hooks.clear
      end

      it 'runs the TLS context hook before connecting' do
        if ENV['OCSP_ALGORITHM']
          skip "OCSP configurations use different certificates which this test does not handle"
        end

        expect(proc).to receive(:call).and_call_original
        socket
        # Even though we are requesting a single cipher in the hook,
        # there may be multiple ciphers available in the context.
        # All of the ciphers should match the requested one (using
        # OpenSSL's idea of what "match" means).
        socket.context.ciphers.each do |cipher|
          unless cipher.first =~ /SHA256/ || cipher.last == 256
            raise "Unexpected cipher #{cipher} after requesting SHA-256"
          end
        end
      end
    end

    context 'when a certificate is provided' do

      context 'when connecting the tcp socket is successful' do

        it 'connects to the server' do
          expect(socket).to be_alive
        end
      end

      context 'when connecting the tcp socket raises an exception' do

        it 'raises an exception' do
          expect_any_instance_of(::Socket).to receive(:connect).and_raise(Mongo::Error::SocketTimeoutError)
          expect do
            socket
          end.to raise_error(Mongo::Error::SocketTimeoutError)
        end
      end
    end

    context 'when a certificate and key are provided as strings' do

      let(:ssl_options) do
        {
          :ssl => true,
          :ssl_cert_string => cert_string,
          :ssl_key_string => key_string,
          :ssl_verify => false
        }
      end

      it 'connects to the server' do
        expect(socket).to be_alive
      end
    end

    context 'when certificate and an encrypted key are provided as strings' do
      require_local_tls

      let(:ssl_options) do
        {
          :ssl => true,
          :ssl_cert_string => cert_string,
          :ssl_key_string => key_encrypted_string,
          :ssl_key_pass_phrase => SpecConfig.instance.client_encrypted_key_passphrase,
          :ssl_verify => false
        }
      end

      it 'connects to the server' do
        expect(socket).to be_alive
      end
    end

    context 'when a certificate and key are provided as objects' do

      let(:ssl_options) do
        {
          :ssl => true,
          :ssl_cert_object => cert_object,
          :ssl_key_object => key_object,
          :ssl_verify => false
        }
      end

      it 'connects to the server' do
        expect(socket).to be_alive
      end
    end

    context 'when the certificate is specified using both a file and a PEM-encoded string' do

      let(:ssl_options) do
        super().merge(
          :ssl_cert_string => 'This is a random string, not a PEM-encoded certificate'
        )
      end

      # since the lower priority option is clearly invalid we verify priority by checking that it connects
      it 'discards the value of :ssl_cert_string' do
        expect(socket).to be_alive
      end
    end

    context 'when the certificate is specified using both a file and an object' do

      let(:ssl_options) do
        super().merge(
          :ssl_cert_object => 'This is a string, not a certificate'
        )
      end

      # since the lower priority option is clearly invalid we verify priority by checking that it connects
      it 'discards the value of :ssl_cert_object' do
        expect(socket).to be_alive
      end
    end

    context 'when the certificate is specified using both a PEM-encoded string and an object' do

      let(:ssl_options) do
        {
          :ssl => true,
          :ssl_cert_string => cert_string,
          :ssl_cert_object => 'This is a string, not a Certificate',
          :ssl_key => SpecConfig.instance.client_key_path,
          :ssl_verify => false
        }
      end

      # since the lower priority option is clearly invalid we verify priority by checking that it connects
      it 'discards the value of :ssl_cert_object' do
        expect(socket).to be_alive
      end
    end

    context 'when the key is specified using both a file and a PEM-encoded string' do

      let(:ssl_options) do
        super().merge(
          :ssl_key_string => 'This is a normal string, not a PEM-encoded key'
        )
      end

      # since the lower priority option is clearly invalid we verify priority by checking that it connects
      it 'discards the value of :ssl_key_string' do
        expect(socket).to be_alive
      end
    end

    context 'when the key is specified using both a file and an object' do

      let(:ssl_options) do
        super().merge(
          :ssl_cert_object => 'This is a string, not a key'
        )
      end

      # since the lower priority option is clearly invalid we verify priority by checking that it connects
      it 'discards the value of :ssl_key_object' do
        expect(socket).to be_alive
      end
    end

    context 'when the key is specified using both a PEM-encoded string and an object' do

      let(:ssl_options) do
        {
          :ssl => true,
          :ssl_cert => SpecConfig.instance.client_cert_path,
          :ssl_key_string => key_string,
          :ssl_key_object => 'This is a string, not a PKey',
          :ssl_verify => false
        }
      end

      # since the lower priority option is clearly invalid we verify priority by checking that it connects
      it 'discards the value of :ssl_key_object' do
        expect(socket).to be_alive
      end
    end

    context 'when a certificate is passed, but it is not of the right type' do

      let(:ssl_options) do
        cert = "This is a string, not an X.509 Certificate"
        {
          :ssl => true,
          :ssl_cert_object => cert,
          :ssl_key => SpecConfig.instance.local_client_key_path,
          :ssl_verify => false
        }
      end

      it 'raises a TypeError' do
        expect do
          socket
        end.to raise_exception(TypeError)
      end
    end

    context 'when the hostname is incorrect' do
      let(:host_name) do
        'incorrect_hostname'
      end

      context 'when the hostname is verified' do

        let(:ssl_options) do
          SpecConfig.instance.ssl_options.merge(ssl_verify: false, ssl_verify_hostname: true)
        end

        it 'raises an error' do
          lambda do
            socket
          end.should raise_error(Mongo::Error::SocketError, /TLS handshake failed due to a hostname mismatch/)
        end
      end

      context 'when the hostname is not verified' do
        let(:ssl_options) do
          SpecConfig.instance.ssl_options.merge(ssl_verify: false, ssl_verify_hostname: false)
        end

        it 'does not raise an error' do
          lambda do
            socket
          end.should_not raise_error
        end
      end
    end

    # Note that as of MRI 2.4, Creating a socket with the wrong key type raises
    # a NoMethodError because #private? is attempted to be called on the key.
    # In jruby 9.2 a TypeError is raised.
    # In jruby 9.1 a OpenSSL::PKey::PKeyError is raised.
    context 'when a key is passed, but it is not of the right type' do

      let(:ssl_options) do
        key = "This is a string not a key"
        {
            :ssl => true,
            :ssl_key_object => key,
            :ssl_cert => SpecConfig.instance.client_cert_path,
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
          # MRI
          if RUBY_VERSION >= '3.1.0'
            TypeError
          else
            NoMethodError
          end
        end
      end

      it 'raises a NoMethodError' do
        expect do
          socket
        end.to raise_exception(expected_exception)
      end
    end

    context 'when a bad certificate/key is provided' do

      shared_examples_for 'raises an exception' do
        it 'raises an exception' do
          expect do
            socket
          end.to raise_exception(*expected_exception)
        end
      end

      context 'mri' do
        require_mri

        context 'when a bad certificate is provided' do

          let(:expected_exception) do
            if RUBY_VERSION >= '3.1.0'
              # OpenSSL::X509::CertificateError: PEM_read_bio_X509: no start line
              OpenSSL::X509::CertificateError
            else
              # OpenSSL::X509::CertificateError: nested asn1 error
              [OpenSSL::OpenSSLError, /asn1 error/i]
            end
          end

          let(:ssl_options) do
            super().merge(
              :ssl_cert => CRUD_TESTS.first,
              :ssl_key => nil,
            )
          end

          it_behaves_like 'raises an exception'
        end

        context 'when a bad key is provided' do

          let(:expected_exception) do
            # OpenSSL::PKey::PKeyError: Could not parse PKey: no start line
            [OpenSSL::OpenSSLError, /Could not parse PKey/]
          end

          let(:ssl_options) do
            super().merge(
              :ssl_cert => nil,
              :ssl_key => CRUD_TESTS.first,
            )
          end

          it_behaves_like 'raises an exception'
        end
      end

      context 'jruby' do
        require_jruby

        # On JRuby the key does not appear to be parsed, therefore only
        # specifying the bad certificate produces an error.

        context 'when a bad certificate is provided' do

          let(:ssl_options) do
            super().merge(
              :ssl_cert => CRUD_TESTS.first,
              :ssl_key => nil,
            )
          end

          let(:expected_exception) do
            # java.lang.ClassCastException: org.bouncycastle.asn1.DERApplicationSpecific cannot be cast to org.bouncycastle.asn1.ASN1Sequence
            # OpenSSL::X509::CertificateError: parsing issue: malformed PEM data: no header found
            [OpenSSL::OpenSSLError, /malformed pem data/i]
          end

          it_behaves_like 'raises an exception'
        end
      end
    end

    context 'when a CA certificate is provided' do
      require_local_tls

      context 'as a path to a file' do

        let(:ssl_options) do
          super().merge(
            :ssl_ca_cert => SpecConfig.instance.local_ca_cert_path,
            :ssl_verify => true
          )
        end

        it 'connects to the server' do
          expect(socket).to be_alive
        end
      end

      context 'as a string containing the PEM-encoded certificate' do

        let(:ssl_options) do
          super().merge(
            :ssl_ca_cert_string => ca_cert_string,
            :ssl_verify => true
          )
        end

        it 'connects to the server' do
          expect(socket).to be_alive
        end
      end

      context 'as an array of Certificate objects' do
        let(:ssl_options) do
          cert = [OpenSSL::X509::Certificate.new(ca_cert_string)]
          super().merge(
            :ssl_ca_cert_object => cert,
            :ssl_verify => true
          )
        end

        it 'connects to the server' do
          expect(socket).to be_alive
        end
      end

      context 'both as a file and a PEM-encoded parameter' do

        let(:ssl_options) do
          super().merge(
            :ssl_ca_cert => SpecConfig.instance.local_ca_cert_path,
            :ssl_ca_cert_string => 'This is a string, not a certificate',
            :ssl_verify => true
          )
        end

        # since the lower priority option is clearly invalid we verify priority by checking that it connects
        it 'discards the value of :ssl_ca_cert_string' do
          expect(socket).to be_alive
        end
      end

      context 'both as a file and as object parameter' do

        let(:ssl_options) do
          super().merge(
            :ssl_ca_cert => SpecConfig.instance.local_ca_cert_path,
            :ssl_ca_cert_object => 'This is a string, not an array of certificates',
            :ssl_verify => true
          )
        end

        it 'discards the value of :ssl_ca_cert_object' do
          expect(socket).to be_alive
        end
      end

      context 'both as a PEM-encoded string and as object parameter' do

        let(:ssl_options) do
          cert = File.read(SpecConfig.instance.local_ca_cert_path)
          super().merge(
            :ssl_ca_cert_string => cert,
            :ssl_ca_cert_object => 'This is a string, not an array of certificates',
            :ssl_verify => true
          )
        end

        it 'discards the value of :ssl_ca_cert_object' do
          expect(socket).to be_alive
        end
      end
    end

    context 'when CA certificate file is not what server cert is signed with' do
      require_local_tls

      let(:server) do
        ClientRegistry.instance.global_client('authorized').cluster.next_primary
      end

      let(:connection) do
        Mongo::Server::Connection.new(server, ssl_options.merge(socket_timeout: 2))
      end

      context 'as a file' do
        let(:ssl_options) do
          SpecConfig.instance.test_options.merge(
            ssl: true,
            ssl_cert: SpecConfig.instance.client_cert_path,
            ssl_key: SpecConfig.instance.client_key_path,
            ssl_ca_cert: SpecConfig.instance.ssl_certs_dir.join('python-ca.crt').to_s,
            ssl_verify: true,
          )
        end

        it 'fails' do
          connection
          expect do
            connection.connect!
          end.to raise_error(Mongo::Error::SocketError, /SSLError/)
        end
      end
    end

    context 'when CA certificate file contains multiple certificates' do
      require_local_tls

      let(:server) do
        ClientRegistry.instance.global_client('authorized').cluster.next_primary
      end

      let(:connection) do
        Mongo::Server::Connection.new(server, ssl_options.merge(socket_timeout: 2))
      end

      context 'as a file' do
        let(:ssl_options) do
          SpecConfig.instance.test_options.merge(
            ssl: true,
            ssl_cert: SpecConfig.instance.client_cert_path,
            ssl_key: SpecConfig.instance.client_key_path,
            ssl_ca_cert: SpecConfig.instance.multi_ca_path,
            ssl_verify: true,
          )
        end

        it 'succeeds' do
          connection
          expect do
            connection.connect!
          end.not_to raise_error
        end
      end
    end

    context 'when a CA certificate is not provided' do
      require_local_tls

      let(:ssl_options) do
        super().merge(
          :ssl_verify => true
        )
      end

      local_env do
        { 'SSL_CERT_FILE' => SpecConfig.instance.local_ca_cert_path }
      end

      it 'uses the default cert store' do
        expect(socket).to be_alive
      end
    end

    context 'when the client certificate uses an intermediate certificate' do
      require_local_tls

      let(:server) do
        ClientRegistry.instance.global_client('authorized').cluster.next_primary
      end

      let(:connection) do
        Mongo::Server::Connection.new(server, ssl_options.merge(socket_timeout: 2))
      end

      context 'as a path to a file' do
        context 'standalone' do
          let(:ssl_options) do
            SpecConfig.instance.test_options.merge(
              ssl_cert: SpecConfig.instance.second_level_cert_path,
              ssl_key: SpecConfig.instance.second_level_key_path,
              ssl_ca_cert: SpecConfig.instance.local_ca_cert_path,
              ssl_verify: true,
            )
          end

          it 'fails' do
            # This test provides a second level client certificate to the
            # server *without* providing the intermediate certificate.
            # If the server performs certificate verification, it will
            # reject the connection (seen from the driver as a SocketError)
            # and the test will succeed. If the server does not perform
            # certificate verification, it will accept the connection,
            # no SocketError will be raised and the test will fail.
            connection
            expect do
              connection.connect!
            end.to raise_error(Mongo::Error::SocketError)
          end
        end

        context 'bundled with intermediate cert' do

          # https://github.com/jruby/jruby-openssl/issues/181
          require_mri

          let(:ssl_options) do
            SpecConfig.instance.test_options.merge(
              ssl: true,
              ssl_cert: SpecConfig.instance.second_level_cert_bundle_path,
              ssl_key: SpecConfig.instance.second_level_key_path,
              ssl_ca_cert: SpecConfig.instance.local_ca_cert_path,
              ssl_verify: true,
            )
          end

          it 'succeeds' do
            connection
            expect do
              connection.connect!
            end.not_to raise_error
          end
        end
      end

      context 'as a string' do
        context 'standalone' do
          let(:ssl_options) do
            SpecConfig.instance.test_options.merge(
              ssl_cert: nil,
              ssl_cert_string: File.read(SpecConfig.instance.second_level_cert_path),
              ssl_key: nil,
              ssl_key_string: File.read(SpecConfig.instance.second_level_key_path),
              ssl_ca_cert: SpecConfig.instance.local_ca_cert_path,
              ssl_verify: true,
            )
          end

          it 'fails' do
            connection
            expect do
              connection.connect!
            end.to raise_error(Mongo::Error::SocketError)
          end
        end

        context 'bundled with intermediate cert' do

          # https://github.com/jruby/jruby-openssl/issues/181
          require_mri

          let(:ssl_options) do
            SpecConfig.instance.test_options.merge(
              ssl: true,
              ssl_cert: nil,
              ssl_cert_string: File.read(SpecConfig.instance.second_level_cert_bundle_path),
              ssl_key: nil,
              ssl_key_string: File.read(SpecConfig.instance.second_level_key_path),
              ssl_ca_cert: SpecConfig.instance.local_ca_cert_path,
              ssl_verify: true,
            )
          end

          it 'succeeds' do
            connection
            expect do
              connection.connect!
            end.not_to raise_error
          end
        end
      end
    end

    context 'when client certificate and private key are bunded in a pem file' do
      require_local_tls

      let(:server) do
        ClientRegistry.instance.global_client('authorized').cluster.next_primary
      end

      let(:connection) do
        Mongo::Server::Connection.new(server, ssl_options.merge(socket_timeout: 2))
      end

      let(:ssl_options) do
        SpecConfig.instance.ssl_options.merge(
          ssl: true,
          ssl_cert: SpecConfig.instance.client_pem_path,
          ssl_key: SpecConfig.instance.client_pem_path,
          ssl_ca_cert: SpecConfig.instance.local_ca_cert_path,
          ssl_verify: true,
        )
      end

      it 'succeeds' do
        connection
        expect do
          connection.connect!
        end.not_to raise_error
      end
    end

    context 'when ssl_verify is not specified' do
      require_local_tls

      let(:ssl_options) do
        super().merge(
          :ssl_ca_cert => SpecConfig.instance.local_ca_cert_path
        ).tap { |options| options.delete(:ssl_verify) }
      end

      it 'verifies the server certificate' do
        expect(socket).to be_alive
      end
    end

    context 'when ssl_verify is true' do
      require_local_tls

      let(:ssl_options) do
        super().merge(
          :ssl_ca_cert => SpecConfig.instance.local_ca_cert_path,
          :ssl_verify => true
        )
      end

      it 'verifies the server certificate' do
        expect(socket).to be_alive
      end
    end

    context 'when ssl_verify is false' do

      let(:ssl_options) do
        super().merge(
          :ssl_ca_cert => 'invalid',
          :ssl_verify => false
        )
      end

      it 'does not verify the server certificate' do
        expect(socket).to be_alive
      end
    end

    context 'when OpenSSL allows disabling renegotiation 'do
      before do
        unless OpenSSL::SSL.const_defined?(:OP_NO_RENEGOTIATION)
          skip 'OpenSSL::SSL::OP_NO_RENEGOTIATION is not defined'
        end
      end

      it 'disables TLS renegotiation' do
        expect(socket.context.options & OpenSSL::SSL::OP_NO_RENEGOTIATION).to eq(OpenSSL::SSL::OP_NO_RENEGOTIATION)
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

      let(:remote_address) { socket.instance_variable_get(:@tcp_socket).remote_address }
      let(:address_str) { "#{remote_address.ip_address}:#{remote_address.ip_port} (#{default_address}, TLS)" }

      it 'should raise EOFError' do
        expect do
          socket.readbyte
        end.to raise_error(Mongo::Error::SocketError).with_message("EOFError: EOFError (for #{address_str})")
      end
    end
  end
end
