# frozen_string_literal: true
# rubocop:todo all

require 'mongo'
require 'lite_spec_helper'

describe Mongo::Crypt::KMS::Credentials do
  require_libmongocrypt
  include_context 'define shared FLE helpers'

  context 'AWS' do
    let (:params) do
      Mongo::Crypt::KMS::AWS::Credentials.new(kms_provider)
    end


    %i(access_key_id secret_access_key).each do |key|
      context "with nil AWS #{key}" do
        let(:kms_provider) do
          {
            access_key_id: SpecConfig.instance.fle_aws_key,
            secret_access_key: SpecConfig.instance.fle_aws_secret,
          }.update({key => nil})
        end

        it 'raises an exception' do
          expect do
            params
          end.to raise_error(ArgumentError, /The #{key} option must be a String with at least one character; currently have nil/)
        end
      end

      context "with non-string AWS #{key}" do
        let(:kms_provider) do
          {
            access_key_id: SpecConfig.instance.fle_aws_key,
            secret_access_key: SpecConfig.instance.fle_aws_secret,
          }.update({key => 5})
        end

        it 'raises an exception' do
          expect do
            params
          end.to raise_error(ArgumentError, /The #{key} option must be a String with at least one character; currently have 5/)
        end
      end

      context "with empty string AWS #{key}" do
        let(:kms_provider) do
          {
            access_key_id: SpecConfig.instance.fle_aws_key,
            secret_access_key: SpecConfig.instance.fle_aws_secret,
          }.update({key => ''})
        end

        it 'raises an exception' do
          expect do
            params
          end.to raise_error(ArgumentError, /The #{key} option must be a String with at least one character; it is currently an empty string/)
        end
      end
    end

    context 'with valid params' do
      let(:kms_provider) do
        {
          access_key_id: SpecConfig.instance.fle_aws_key,
          secret_access_key: SpecConfig.instance.fle_aws_secret,
        }
      end

      it 'returns valid libmongocrypt credentials' do
        expect(params.to_document).to eq(
          BSON::Document.new({
            accessKeyId: SpecConfig.instance.fle_aws_key,
            secretAccessKey: SpecConfig.instance.fle_aws_secret,
          })
        )
      end
    end
  end

  context 'Azure' do
    let (:params) do
      Mongo::Crypt::KMS::Azure::Credentials.new(kms_provider)
    end

    %i(tenant_id client_id client_secret).each do |param|

      context "with nil azure #{param}" do
        let(:kms_provider) do
          {
            tenant_id: SpecConfig.instance.fle_azure_tenant_id,
            client_id: SpecConfig.instance.fle_azure_client_id,
            client_secret: SpecConfig.instance.fle_azure_client_secret
          }.update(param => nil)
        end

        it 'raises an exception' do
          expect do
            params
          end.to raise_error(ArgumentError, /The #{param} option must be a String with at least one character; currently have nil/)
        end
      end

      context "with non-string azure #{param}" do
        let(:kms_provider) do
          {
            tenant_id: SpecConfig.instance.fle_azure_tenant_id,
            client_id: SpecConfig.instance.fle_azure_client_id,
            client_secret: SpecConfig.instance.fle_azure_client_secret
          }.update(param => 5)
        end

        it 'raises an exception' do
          expect do
            params
          end.to raise_error(ArgumentError, /The #{param} option must be a String with at least one character; currently have 5/)
        end
      end

      context "with empty string azure #{param}" do
        let(:kms_provider) do
          {
            tenant_id: SpecConfig.instance.fle_azure_tenant_id,
            client_id: SpecConfig.instance.fle_azure_client_id,
            client_secret: SpecConfig.instance.fle_azure_client_secret
          }.update(param => '')
        end

        it 'raises an exception' do
          expect do
            params
          end.to raise_error(ArgumentError, /The #{param} option must be a String with at least one character; it is currently an empty string/)
        end
      end
    end

    context "with non-string azure identity_platform_endpoint" do
      let(:kms_provider) do
        {
          tenant_id: SpecConfig.instance.fle_azure_tenant_id,
          client_id: SpecConfig.instance.fle_azure_client_id,
          client_secret: SpecConfig.instance.fle_azure_client_secret,
          identity_platform_endpoint: 5
        }
      end

      it 'raises an exception' do
        expect do
          params
        end.to raise_error(ArgumentError, /The identity_platform_endpoint option must be a String with at least one character; currently have 5/)
      end
    end

    context "with empty string azure identity_platform_endpoint" do
      let(:kms_provider) do
        {
          tenant_id: SpecConfig.instance.fle_azure_tenant_id,
          client_id: SpecConfig.instance.fle_azure_client_id,
          client_secret: SpecConfig.instance.fle_azure_client_secret,
          identity_platform_endpoint: ''
        }
      end

      it 'raises an exception' do
        expect do
          params
        end.to raise_error(ArgumentError, /The identity_platform_endpoint option must be a String with at least one character; it is currently an empty string/)
      end
    end

    context 'with valid params' do
      let(:kms_provider) do
        {
          tenant_id: SpecConfig.instance.fle_azure_tenant_id,
          client_id: SpecConfig.instance.fle_azure_client_id,
          client_secret: SpecConfig.instance.fle_azure_client_secret,
        }
      end

      it 'returns valid libmongocrypt credentials' do
        expect(params.to_document).to eq(
          BSON::Document.new({
            tenantId: SpecConfig.instance.fle_azure_tenant_id,
            clientId: SpecConfig.instance.fle_azure_client_id,
            clientSecret: SpecConfig.instance.fle_azure_client_secret,
          })
        )
      end
    end
  end

  context 'GCP' do
    let (:params) do
      Mongo::Crypt::KMS::GCP::Credentials.new(kms_provider)
    end

    %i(email private_key).each do |key|
      context "with nil GCP #{key}" do
        let(:kms_provider) do
          {
            email: SpecConfig.instance.fle_gcp_email,
            private_key: SpecConfig.instance.fle_gcp_private_key,
          }.update({key => nil})
        end

        it 'raises an exception' do
          expect do
            params
          end.to raise_error(ArgumentError, /The #{key} option must be a String with at least one character; currently have nil/)
        end
      end

      context "with non-string GCP #{key}" do
        let(:kms_provider) do
          {
            email: SpecConfig.instance.fle_gcp_email,
            private_key: SpecConfig.instance.fle_gcp_private_key,
          }.update({key => 5})
        end

        it 'raises an exception' do
          expect do
            params
          end.to raise_error(ArgumentError, /The #{key} option must be a String with at least one character; currently have 5/)
        end
      end

      context "with empty string GCP #{key}" do
        let(:kms_provider) do
          {
            email: SpecConfig.instance.fle_gcp_email,
            private_key: SpecConfig.instance.fle_gcp_private_key,
          }.update({key => ''})
        end

        it 'raises an exception' do
          expect do
            params
          end.to raise_error(ArgumentError, /The #{key} option must be a String with at least one character; it is currently an empty string/)
        end
      end
    end

    context 'with valid params' do
      let(:kms_provider) do
        {
          email: SpecConfig.instance.fle_gcp_email,
          private_key: SpecConfig.instance.fle_gcp_private_key,
      }
      end

      it 'returns valid libmongocrypt credentials' do
        expect(params.to_document).to eq(
          BSON::Document.new({
            email: SpecConfig.instance.fle_gcp_email,
            privateKey: BSON::Binary.new(SpecConfig.instance.fle_gcp_private_key, :generic),
          })
        )
      end

      context 'PEM private key' do
        require_mri
        before(:all) do
          if RUBY_VERSION < "3.0"
            skip "Ruby version 3.0 or higher required"
          end
        end

        let(:private_key_pem) do
          OpenSSL::PKey.read(
            Base64.decode64(SpecConfig.instance.fle_gcp_private_key)
          ).export
        end

        let(:kms_provider) do
          {
            email: SpecConfig.instance.fle_gcp_email,
            private_key: private_key_pem,
        }
        end

        it 'returns valid libmongocrypt credentials' do
          private_key = params.to_document[:privateKey]
          expect(Base64.decode64(private_key.data)).to eq(
            Base64.decode64(SpecConfig.instance.fle_gcp_private_key)
          )
        end
      end
    end

    context 'with access token' do
      let(:kms_provider) do
        {
          access_token: 'access_token'
        }
      end

      it 'returns valid libmongocrypt credentials' do
        expect(params.to_document).to eq(
          BSON::Document.new({
            accessToken: 'access_token'
          })
        )
      end
    end
  end

  context 'KMIP' do
    let (:params) do
      Mongo::Crypt::KMS::KMIP::Credentials.new(kms_provider)
    end

    context "with nil KMIP endpoint" do
      let(:kms_provider) do
        {
          endpoint: nil
        }
      end

      it 'raises an exception' do
        expect do
          params
        end.to raise_error(ArgumentError, /The endpoint option must be a String with at least one character; currently have nil/)
      end
    end

    context "with non-string KMIP endpoint" do
      let(:kms_provider) do
        {
          endpoint: 5,
        }
      end

      it 'raises an exception' do
        expect do
          params
        end.to raise_error(ArgumentError, /The endpoint option must be a String with at least one character; currently have 5/)
      end
    end

    context "with empty string KMIP endpoint" do
      let(:kms_provider) do
        {
          endpoint: '',
        }
      end

      it 'raises an exception' do
        expect do
          params
        end.to raise_error(ArgumentError, /The endpoint option must be a String with at least one character; it is currently an empty string/)
      end
    end

    context 'with valid params' do
      let(:kms_provider) do
        {
        endpoint: SpecConfig.instance.fle_kmip_endpoint,
        }
      end

      it 'returns valid libmongocrypt credentials' do
        expect(params.to_document).to eq(
          BSON::Document.new({
            endpoint: SpecConfig.instance.fle_kmip_endpoint,
          })
        )
      end
    end
  end
end
