# frozen_string_literal: true
# encoding: utf-8

require 'mongo'
require 'lite_spec_helper'

describe Mongo::Crypt::KMS::Credentials do
  require_libmongocrypt
  include_context 'define shared FLE helpers'

  context 'AWS' do
    let (:params) do
      Mongo::Crypt::KMS::AWS::Credentials.new(kms_provider)
    end

    context 'with empty AWS kms_provider' do
      let(:kms_provider) do
        {}
      end

      it 'raises an exception' do
        expect do
          params
        end.to raise_error(ArgumentError, /The specified KMS provider options are invalid: {}. AWS KMS provider options must be in the format: { access_key_id: 'YOUR-ACCESS-KEY-ID', secret_access_key: 'SECRET-ACCESS-KEY' }/)
      end
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

    context 'with empty Azure kms_provider' do
      let(:kms_provider) do
        {}
      end

      it 'raises an exception' do
        expect do
          params
        end.to raise_error(ArgumentError, /The specified KMS provider options are invalid: {}. Azure KMS provider options must be in the format: { tenant_id: 'TENANT-ID', client_id: 'TENANT_ID', client_secret: 'CLIENT_SECRET' }/)
      end
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
end
