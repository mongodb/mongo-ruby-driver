# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'
require 'webrick'

describe Mongo::Socket::OcspVerifier do
  require_ocsp_verifier

  shared_examples 'verifies' do
    context 'mri' do
      fails_on_jruby

      it 'verifies the first time and reads from cache the second time' do
        RSpec::Mocks.with_temporary_scope do
          expect_any_instance_of(Mongo::Socket::OcspVerifier).to receive(:do_verify).and_call_original

          verifier.verify_with_cache.should be true
        end

        RSpec::Mocks.with_temporary_scope do
          expect_any_instance_of(Mongo::Socket::OcspVerifier).not_to receive(:do_verify)

          verifier.verify_with_cache.should be true
        end
      end
    end

    context 'jruby' do
      require_jruby

      # JRuby does not return OCSP endpoints, therefore we never perform
      # any validation.
      # https://github.com/jruby/jruby-openssl/issues/210
      it 'does not verify' do
        RSpec::Mocks.with_temporary_scope do
          expect_any_instance_of(Mongo::Socket::OcspVerifier).to receive(:do_verify).and_call_original

          verifier.verify.should be false
        end

        RSpec::Mocks.with_temporary_scope do
          expect_any_instance_of(Mongo::Socket::OcspVerifier).to receive(:do_verify).and_call_original

          verifier.verify.should be false
        end
      end
    end
  end

  shared_examples 'fails verification' do
    context 'mri' do
      fails_on_jruby

      it 'verifies the first time, reads from cache the second time, raises an exception in both cases' do
        RSpec::Mocks.with_temporary_scope do
          expect_any_instance_of(Mongo::Socket::OcspVerifier).to receive(:do_verify).and_call_original

          lambda do
            verifier.verify
          # Redirect tests receive responses from port 8101,
          # tests without redirects receive responses from port 8100.
          end.should raise_error(Mongo::Error::ServerCertificateRevoked, %r,TLS certificate of 'foo' has been revoked according to 'http://localhost:810[01]/status',)
        end

        RSpec::Mocks.with_temporary_scope do
          expect_any_instance_of(Mongo::Socket::OcspVerifier).not_to receive(:do_verify)

          lambda do
            verifier.verify
          # Redirect tests receive responses from port 8101,
          # tests without redirects receive responses from port 8100.
          end.should raise_error(Mongo::Error::ServerCertificateRevoked, %r,TLS certificate of 'foo' has been revoked according to 'http://localhost:810[01]/status',)
        end
      end
    end

    context 'jruby' do
      require_jruby

      # JRuby does not return OCSP endpoints, therefore we never perform
      # any validation.
      # https://github.com/jruby/jruby-openssl/issues/210
      it 'does not verify' do
        RSpec::Mocks.with_temporary_scope do
          expect_any_instance_of(Mongo::Socket::OcspVerifier).to receive(:do_verify).and_call_original

          verifier.verify.should be false
        end

        RSpec::Mocks.with_temporary_scope do
          expect_any_instance_of(Mongo::Socket::OcspVerifier).to receive(:do_verify).and_call_original

          verifier.verify.should be false
        end
      end
    end
  end

  shared_examples 'does not verify' do
    it 'does not verify and does not raise an exception' do
      RSpec::Mocks.with_temporary_scope do
        expect_any_instance_of(Mongo::Socket::OcspVerifier).to receive(:do_verify).and_call_original

        verifier.verify.should be false
      end

      RSpec::Mocks.with_temporary_scope do
        expect_any_instance_of(Mongo::Socket::OcspVerifier).to receive(:do_verify).and_call_original

        verifier.verify.should be false
      end
    end
  end

  shared_context 'verifier' do |opts|
    algorithm = opts[:algorithm]

    let(:cert_path) { SpecConfig.instance.ocsp_files_dir.join("#{algorithm}/server.pem") }
    let(:ca_cert_path) { SpecConfig.instance.ocsp_files_dir.join("#{algorithm}/ca.pem") }

    let(:cert) { OpenSSL::X509::Certificate.new(File.read(cert_path)) }
    let(:ca_cert) { OpenSSL::X509::Certificate.new(File.read(ca_cert_path)) }

    let(:cert_store) do
      OpenSSL::X509::Store.new.tap do |store|
        store.add_cert(ca_cert)
      end
    end

    let(:verifier) do
      described_class.new('foo', cert, ca_cert, cert_store, timeout: 3)
    end
  end

  include_context 'verifier', algorithm: 'rsa'
  algorithm = 'rsa'

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
          end.should take_shorter_than 3
        end
      end
    end
  end
end
