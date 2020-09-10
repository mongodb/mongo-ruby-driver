require 'lite_spec_helper'

describe Mongo::Socket::OcspVerifier do
  require_ocsp_verifier

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
        end.should raise_error(Mongo::Error::ServerCertificateRevoked, %r,TLS certificate of 'foo' has been revoked according to 'http://localhost:8100/status',)
      end

      it 'does not wait for the timeout' do
        lambda do
          lambda do
            verifier.verify
          end.should raise_error(Mongo::Error::ServerCertificateRevoked)
        end.should take_shorter_than 3
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

  %w(rsa ecdsa).each do |algorithm|
    context "when using #{algorithm} cert" do
      let(:cert_path) { SpecConfig.instance.ocsp_files_dir.join("#{algorithm}/server.pem") }
      let(:ca_cert_path) { SpecConfig.instance.ocsp_files_dir.join("#{algorithm}/ca.pem") }

      let(:cert) { OpenSSL::X509::Certificate.new(File.read(cert_path)) }
      let(:ca_cert) { OpenSSL::X509::Certificate.new(File.read(ca_cert_path)) }

      let(:verifier) do
        described_class.new('foo', cert, ca_cert, timeout: 3)
      end

      context 'responder not responding' do
        include_examples 'does not verify'

        it 'does not wait for the timeout' do
          # Loopback interface should be refusing connections, which will make
          # the operation complete quickly.
          lambda do
            verifier.verify
          end.should take_shorter_than 3
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
              end.should take_shorter_than 3
            end
          end

          context 'revoked response' do
            with_ocsp_mock(
              SpecConfig.instance.ocsp_files_dir.join("#{algorithm}/ca.pem"),
              SpecConfig.instance.ocsp_files_dir.join("#{algorithm}/#{responder_cert_file_name}.crt"),
              SpecConfig.instance.ocsp_files_dir.join("#{algorithm}/#{responder_cert_file_name}.key"),
              'revoked'
            )

            include_examples 'fails verification'
          end

          context 'unknown response' do
            with_ocsp_mock(
              SpecConfig.instance.ocsp_files_dir.join("#{algorithm}/ca.pem"),
              SpecConfig.instance.ocsp_files_dir.join("#{algorithm}/#{responder_cert_file_name}.crt"),
              SpecConfig.instance.ocsp_files_dir.join("#{algorithm}/#{responder_cert_file_name}.key"),
              'unknown',
            )

            include_examples 'does not verify'

            it 'does not wait for the timeout' do
              lambda do
                verifier.verify
              end.should take_shorter_than 3
            end
          end
        end
      end
    end
  end
end
