require 'lite_spec_helper'

describe Mongo::Socket::OcspVerifier do
  require_ocsp

  let(:cert_path) { SpecConfig.instance.ocsp_files_dir.join('rsa/server.pem') }
  let(:ca_cert_path) { SpecConfig.instance.ocsp_files_dir.join('rsa/ca.pem') }

  let(:cert) { OpenSSL::X509::Certificate.new(File.read(cert_path)) }
  let(:ca_cert) { OpenSSL::X509::Certificate.new(File.read(ca_cert_path)) }

  let(:verifier) do
    described_class.new('foo', cert, ca_cert, timeout: 3)
  end

  context 'responder not responding' do
    it 'returns false but does not raise' do
      verifier.verify.should be false
    end

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
          SpecConfig.instance.ocsp_files_dir.join('rsa/ca.pem'),
          SpecConfig.instance.ocsp_files_dir.join("rsa/#{responder_cert_file_name}.crt"),
          SpecConfig.instance.ocsp_files_dir.join("rsa/#{responder_cert_file_name}.key"),
        )

        it 'verifies' do
          verifier.verify.should be true
        end

        it 'does not wait for the timeout' do
          lambda do
            verifier.verify
          end.should take_shorter_than 3
        end
      end

      context 'revoked response' do
        with_ocsp_mock(
          SpecConfig.instance.ocsp_files_dir.join('rsa/ca.pem'),
          SpecConfig.instance.ocsp_files_dir.join("rsa/#{responder_cert_file_name}.crt"),
          SpecConfig.instance.ocsp_files_dir.join("rsa/#{responder_cert_file_name}.key"),
          'revoked'
        )

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

      context 'unknown response' do
        with_ocsp_mock(
          SpecConfig.instance.ocsp_files_dir.join('rsa/ca.pem'),
          SpecConfig.instance.ocsp_files_dir.join("rsa/#{responder_cert_file_name}.crt"),
          SpecConfig.instance.ocsp_files_dir.join("rsa/#{responder_cert_file_name}.key"),
          'unknown',
        )

        it 'does not verify and does not raise an exception' do
          verifier.verify.should be false
        end

        it 'does not wait for the timeout' do
          lambda do
            verifier.verify
          end.should take_shorter_than 3
        end
      end
    end
  end
end
