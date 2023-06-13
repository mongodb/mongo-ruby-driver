# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'
require 'webrick'

describe Mongo::Socket::OcspVerifier do
  require_ocsp_verifier
  with_openssl_debug
  retry_test sleep: 5

  shared_examples 'verifies' do
    context 'mri' do
      fails_on_jruby

      it 'verifies' do
        verifier.verify.should be true
      end
    end

    context 'jruby' do
      require_jruby

      # JRuby does not return OCSP endpoints, therefore we never perform
      # any validation.
      # https://github.com/jruby/jruby-openssl/issues/210
      it 'does not verify' do
        verifier.verify.should be false
      end
    end
  end

  shared_examples 'fails verification' do
    context 'mri' do
      fails_on_jruby

      it 'raises an exception' do
        lambda do
          verifier.verify
        # Redirect tests receive responses from port 8101,
        # tests without redirects receive responses from port 8100.
        end.should raise_error(Mongo::Error::ServerCertificateRevoked, %r,TLS certificate of 'foo' has been revoked according to 'http://localhost:810[01]/status',)
      end

      it 'does not wait for the timeout' do
        lambda do
          lambda do
            verifier.verify
          end.should raise_error(Mongo::Error::ServerCertificateRevoked)
        end.should take_shorter_than 7
      end
    end

    context 'jruby' do
      require_jruby

      # JRuby does not return OCSP endpoints, therefore we never perform
      # any validation.
      # https://github.com/jruby/jruby-openssl/issues/210
      it 'does not verify' do
        verifier.verify.should be false
      end
    end
  end

  shared_examples 'does not verify' do
    it 'does not verify and does not raise an exception' do
      verifier.verify.should be false
    end
  end

  shared_context 'basic verifier' do

    let(:cert) { OpenSSL::X509::Certificate.new(File.read(cert_path)) }
    let(:ca_cert) { OpenSSL::X509::Certificate.new(File.read(ca_cert_path)) }

    let(:cert_store) do
      OpenSSL::X509::Store.new.tap do |store|
        store.add_cert(ca_cert)
      end
    end

    let(:verifier) do
      described_class.new('foo', cert, ca_cert, cert_store, timeout: 7)
    end
  end

  shared_context 'verifier' do |opts|
    algorithm = opts[:algorithm]

    let(:cert_path) { SpecConfig.instance.ocsp_files_dir.join("#{algorithm}/server.pem") }
    let(:ca_cert_path) { SpecConfig.instance.ocsp_files_dir.join("#{algorithm}/ca.pem") }

    include_context 'basic verifier'
  end

  %w(rsa ecdsa).each do |algorithm|
    context "when using #{algorithm} cert" do
      include_context 'verifier', algorithm: algorithm

      context 'responder not responding' do
        include_examples 'does not verify'

        it 'does not wait for the timeout' do
          # Loopback interface should be refusing connections, which will make
          # the operation complete quickly.
          lambda do
            verifier.verify
          end.should take_shorter_than 7
        end
      end

      %w(ca delegate).each do |responder_cert|
        responder_cert_file_name = {
          'ca' => 'ca',
          'delegate' => 'ocsp-responder',
        }.fetch(responder_cert)

        context "when responder uses #{responder_cert} cert" do
          context 'good response' do
            with_ocsp_mock(
              SpecConfig.instance.ocsp_files_dir.join("#{algorithm}/ca.pem"),
              SpecConfig.instance.ocsp_files_dir.join("#{algorithm}/#{responder_cert_file_name}.crt"),
              SpecConfig.instance.ocsp_files_dir.join("#{algorithm}/#{responder_cert_file_name}.key"),
            )

            include_examples 'verifies'

            it 'does not wait for the timeout' do
              lambda do
                verifier.verify
              end.should take_shorter_than 7
            end
          end

          context 'revoked response' do
            with_ocsp_mock(
              SpecConfig.instance.ocsp_files_dir.join("#{algorithm}/ca.pem"),
              SpecConfig.instance.ocsp_files_dir.join("#{algorithm}/#{responder_cert_file_name}.crt"),
              SpecConfig.instance.ocsp_files_dir.join("#{algorithm}/#{responder_cert_file_name}.key"),
              fault: 'revoked'
            )

            include_examples 'fails verification'
          end

          context 'unknown response' do
            with_ocsp_mock(
              SpecConfig.instance.ocsp_files_dir.join("#{algorithm}/ca.pem"),
              SpecConfig.instance.ocsp_files_dir.join("#{algorithm}/#{responder_cert_file_name}.crt"),
              SpecConfig.instance.ocsp_files_dir.join("#{algorithm}/#{responder_cert_file_name}.key"),
              fault: 'unknown',
            )

            include_examples 'does not verify'

            it 'does not wait for the timeout' do
              lambda do
                verifier.verify
              end.should take_shorter_than 7
            end
          end
        end
      end
    end
  end

  context 'when OCSP responder redirects' do
    algorithm = 'rsa'
    responder_cert_file_name = 'ca'
    let(:algorithm) { 'rsa' }
    let(:responder_cert_file_name) { 'ca' }

    context 'one time' do

      around do |example|
        server = WEBrick::HTTPServer.new(Port: 8100)
        server.mount_proc '/' do |req, res|
          res.status = 303
          res['locAtion'] = "http://localhost:8101#{req.path}"
          res.body = "See http://localhost:8101#{req.path}"
        end
        Thread.new { server.start }
        begin
          example.run
        ensure
          server.shutdown
        end

        ::Utils.wait_for_port_free(8100, 5)
      end

      include_context 'verifier', algorithm: algorithm

      context 'good response' do
        with_ocsp_mock(
          SpecConfig.instance.ocsp_files_dir.join("#{algorithm}/ca.pem"),
          SpecConfig.instance.ocsp_files_dir.join("#{algorithm}/#{responder_cert_file_name}.crt"),
          SpecConfig.instance.ocsp_files_dir.join("#{algorithm}/#{responder_cert_file_name}.key"),
          port: 8101,
        )

        include_examples 'verifies'

        it 'does not wait for the timeout' do
          lambda do
            verifier.verify
          end.should take_shorter_than 7
        end
      end

      context 'revoked response' do
        with_ocsp_mock(
          SpecConfig.instance.ocsp_files_dir.join("#{algorithm}/ca.pem"),
          SpecConfig.instance.ocsp_files_dir.join("#{algorithm}/#{responder_cert_file_name}.crt"),
          SpecConfig.instance.ocsp_files_dir.join("#{algorithm}/#{responder_cert_file_name}.key"),
          fault: 'revoked',
          port: 8101,
        )

        include_examples 'fails verification'
      end

      context 'unknown response' do
        with_ocsp_mock(
          SpecConfig.instance.ocsp_files_dir.join("#{algorithm}/ca.pem"),
          SpecConfig.instance.ocsp_files_dir.join("#{algorithm}/#{responder_cert_file_name}.crt"),
          SpecConfig.instance.ocsp_files_dir.join("#{algorithm}/#{responder_cert_file_name}.key"),
          fault: 'unknown',
          port: 8101,
        )

        include_examples 'does not verify'

        it 'does not wait for the timeout' do
          lambda do
            verifier.verify
          end.should take_shorter_than 7
        end
      end
    end

    context 'infinitely' do
      with_ocsp_mock(
        SpecConfig.instance.ocsp_files_dir.join("#{algorithm}/ca.pem"),
        SpecConfig.instance.ocsp_files_dir.join("#{algorithm}/#{responder_cert_file_name}.crt"),
        SpecConfig.instance.ocsp_files_dir.join("#{algorithm}/#{responder_cert_file_name}.key"),
        port: 8101,
      )

      around do |example|
        server = WEBrick::HTTPServer.new(Port: 8100)
        server.mount_proc '/' do |req, res|
          res.status = 303
          res['locAtion'] = req.path
          res.body = "See #{req.path} indefinitely"
        end
        Thread.new { server.start }
        begin
          example.run
        ensure
          server.shutdown
        end

        ::Utils.wait_for_port_free(8100, 5)
      end

      include_context 'verifier', algorithm: algorithm
      include_examples 'does not verify'
    end
  end

  context 'responder returns unexpected status code' do

    include_context 'verifier', algorithm: 'rsa'

    context '40x / 50x' do
      around do |example|
        server = WEBrick::HTTPServer.new(Port: 8100)
        server.mount_proc '/' do |req, res|
          res.status = code
          res.body = "HTTP #{code}"
        end
        Thread.new { server.start }
        begin
          example.run
        ensure
          server.shutdown
        end

        ::Utils.wait_for_port_free(8100, 5)
      end

      [400, 404, 500, 503].each do |_code|
        context "code #{_code}" do
          let(:code) { _code }
          include_examples 'does not verify'
        end
      end
    end

    context '204' do
      around do |example|
        server = WEBrick::HTTPServer.new(Port: 8100)
        server.mount_proc '/' do |req, res|
          res.status = 204
        end
        Thread.new { server.start }
        begin
          example.run
        ensure
          server.shutdown
        end

        ::Utils.wait_for_port_free(8100, 5)
      end

      context "code 204" do
        let(:code) { 204 }
        include_examples 'does not verify'
      end
    end
  end

  context 'responder URI has no path' do
    require_external_connectivity

    # https://github.com/jruby/jruby-openssl/issues/210
    fails_on_jruby

    include_context 'basic verifier'

    # The fake certificates all have paths in them for use with the ocsp mock.
    # Use real certificates retrieved from Atlas for this test as they don't
    # have a path in the OCSP URI (which the test also asserts).
    # Note that these certificates expire in 3 months and need to be replaced
    # with a more permanent solution.
    # Use the spec/support/certificates/retrieve-atlas-cert script to retrieve
    # current certificates from Atlas.
    let(:cert_path) { File.join(File.dirname(__FILE__), '../support/certificates/atlas-ocsp.crt') }
    let(:ca_cert_path) { File.join(File.dirname(__FILE__), '../support/certificates/atlas-ocsp-ca.crt') }
    let(:cert_store) do
      OpenSSL::X509::Store.new.tap do |store|
        store.set_default_paths
      end
    end

    before do
      verifier.ocsp_uris.length.should > 0
      URI.parse(verifier.ocsp_uris.first).path.should == ''
    end

    it 'verifies' do
      # TODO This test will fail if the certificate expires
      expect(verifier.verify).to be(true), "If atlas-ocsp certificates have expired, run spec/support/certificates/retrieve-atlas-cert to get a new ones"
    end
  end
end
