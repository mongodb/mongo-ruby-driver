# frozen_string_literal: true

require 'mongo'
require 'lite_spec_helper'

describe Mongo::Crypt::KMS::Credentials do
  require_libmongocrypt
  include_context 'define shared FLE helpers'

  context 'AWS' do
    let(:params) do
      Mongo::Crypt::KMS::AWS::Credentials.new(kms_provider)
    end

    %i[access_key_id secret_access_key].each do |key|
      context "with nil AWS #{key}" do
        let(:kms_provider) do
          {
            access_key_id: SpecConfig.instance.fle_aws_key,
            secret_access_key: SpecConfig.instance.fle_aws_secret,
          }.update({ key => nil })
        end

        it 'raises an exception' do
          expect do
            params
          end.to raise_error(ArgumentError,
                             /The #{key} option must be a String with at least one character; currently have nil/)
        end
      end

      context "with non-string AWS #{key}" do
        let(:kms_provider) do
          {
            access_key_id: SpecConfig.instance.fle_aws_key,
            secret_access_key: SpecConfig.instance.fle_aws_secret,
          }.update({ key => 5 })
        end

        it 'raises an exception' do
          expect do
            params
          end.to raise_error(ArgumentError,
                             /The #{key} option must be a String with at least one character; currently have 5/)
        end
      end

      context "with empty string AWS #{key}" do
        let(:kms_provider) do
          {
            access_key_id: SpecConfig.instance.fle_aws_key,
            secret_access_key: SpecConfig.instance.fle_aws_secret,
          }.update({ key => '' })
        end

        it 'raises an exception' do
          expect do
            params
          end.to raise_error(ArgumentError,
                             /The #{key} option must be a String with at least one character; it is currently an empty string/)
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
    let(:params) do
      Mongo::Crypt::KMS::Azure::Credentials.new(kms_provider)
    end

    %i[tenant_id client_id client_secret].each do |param|
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
          end.to raise_error(ArgumentError,
                             /The #{param} option must be a String with at least one character; currently have nil/)
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
          end.to raise_error(ArgumentError,
                             /The #{param} option must be a String with at least one character; currently have 5/)
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
          end.to raise_error(ArgumentError,
                             /The #{param} option must be a String with at least one character; it is currently an empty string/)
        end
      end
    end

    context 'with non-string azure identity_platform_endpoint' do
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
        end.to raise_error(ArgumentError,
                           /The identity_platform_endpoint option must be a String with at least one character; currently have 5/)
      end
    end

    context 'with empty string azure identity_platform_endpoint' do
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
        end.to raise_error(ArgumentError,
                           /The identity_platform_endpoint option must be a String with at least one character; it is currently an empty string/)
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
    let(:params) do
      Mongo::Crypt::KMS::GCP::Credentials.new(kms_provider)
    end

    %i[email private_key].each do |key|
      context "with nil GCP #{key}" do
        let(:kms_provider) do
          {
            email: SpecConfig.instance.fle_gcp_email,
            private_key: SpecConfig.instance.fle_gcp_private_key,
          }.update({ key => nil })
        end

        it 'raises an exception' do
          expect do
            params
          end.to raise_error(ArgumentError,
                             /The #{key} option must be a String with at least one character; currently have nil/)
        end
      end

      context "with non-string GCP #{key}" do
        let(:kms_provider) do
          {
            email: SpecConfig.instance.fle_gcp_email,
            private_key: SpecConfig.instance.fle_gcp_private_key,
          }.update({ key => 5 })
        end

        it 'raises an exception' do
          expect do
            params
          end.to raise_error(ArgumentError,
                             /The #{key} option must be a String with at least one character; currently have 5/)
        end
      end

      context "with empty string GCP #{key}" do
        let(:kms_provider) do
          {
            email: SpecConfig.instance.fle_gcp_email,
            private_key: SpecConfig.instance.fle_gcp_private_key,
          }.update({ key => '' })
        end

        it 'raises an exception' do
          expect do
            params
          end.to raise_error(ArgumentError,
                             /The #{key} option must be a String with at least one character; it is currently an empty string/)
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
          skip 'Ruby version 3.0 or higher required' if RUBY_VERSION < '3.0'
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
    let(:params) do
      Mongo::Crypt::KMS::KMIP::Credentials.new(kms_provider)
    end

    context 'with nil KMIP endpoint' do
      let(:kms_provider) do
        {
          endpoint: nil
        }
      end

      it 'raises an exception' do
        expect do
          params
        end.to raise_error(ArgumentError,
                           /The endpoint option must be a String with at least one character; currently have nil/)
      end
    end

    context 'with non-string KMIP endpoint' do
      let(:kms_provider) do
        {
          endpoint: 5,
        }
      end

      it 'raises an exception' do
        expect do
          params
        end.to raise_error(ArgumentError,
                           /The endpoint option must be a String with at least one character; currently have 5/)
      end
    end

    context 'with empty string KMIP endpoint' do
      let(:kms_provider) do
        {
          endpoint: '',
        }
      end

      it 'raises an exception' do
        expect do
          params
        end.to raise_error(ArgumentError,
                           /The endpoint option must be a String with at least one character; it is currently an empty string/)
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

  context 'named providers' do
    let(:local_key) { Crypt::LOCAL_MASTER_KEY }

    context 'with a single named local provider' do
      let(:kms_providers) { { 'local:name1' => { key: local_key } } }

      it 'initializes without error' do
        expect { Mongo::Crypt::KMS::Credentials.new(kms_providers) }.not_to raise_error
      end

      it 'to_document uses the full named identifier as key' do
        creds = Mongo::Crypt::KMS::Credentials.new(kms_providers)
        doc = creds.to_document
        expect(doc.keys).to include('local:name1')
        expect(doc.keys).not_to include('local')
      end

      it 'unnamed accessor returns nil' do
        creds = Mongo::Crypt::KMS::Credentials.new(kms_providers)
        expect(creds.local).to be_nil
      end
    end

    context 'with both unnamed and named providers of the same type' do
      let(:kms_providers) do
        {
          local: { key: local_key },
          'local:name1' => { key: local_key }
        }
      end

      it 'includes both in to_document' do
        creds = Mongo::Crypt::KMS::Credentials.new(kms_providers)
        doc = creds.to_document
        expect(doc.keys).to include('local', 'local:name1')
      end

      it 'unnamed accessor returns the unnamed credential' do
        creds = Mongo::Crypt::KMS::Credentials.new(kms_providers)
        expect(creds.local).not_to be_nil
      end
    end

    context 'with an unknown provider type' do
      let(:kms_providers) { { 'badtype:name1' => { key: 'something' } } }

      it 'raises ArgumentError' do
        expect do
          Mongo::Crypt::KMS::Credentials.new(kms_providers)
        end.to raise_error(ArgumentError, /must have one of the following keys/)
      end
    end

    context 'with an empty hash' do
      it 'raises ArgumentError' do
        expect do
          Mongo::Crypt::KMS::Credentials.new({})
        end.to raise_error(ArgumentError, /must have one of the following keys/)
      end
    end
  end

  describe Mongo::Crypt::KMS::MasterKeyDocument do
    require_libmongocrypt

    describe '#initialize' do
      context 'with unnamed local provider' do
        it 'succeeds' do
          doc = Mongo::Crypt::KMS::MasterKeyDocument.new('local', {})
          expect(doc.to_document[:provider]).to eq('local')
        end
      end

      context 'with named local provider' do
        it 'succeeds and preserves full identifier in document' do
          doc = Mongo::Crypt::KMS::MasterKeyDocument.new('local:name1', {})
          expect(doc.to_document[:provider]).to eq('local:name1')
        end
      end

      context 'with unknown provider type' do
        it 'raises ArgumentError' do
          expect do
            Mongo::Crypt::KMS::MasterKeyDocument.new('badtype:name1', {})
          end.to raise_error(ArgumentError, /KMS provider must be one of/)
        end
      end

      context 'with named AWS provider' do
        let(:master_key) do
          {
            master_key: {
              region: 'us-east-1',
              key: 'arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0'
            }
          }
        end

        it 'preserves full identifier in document' do
          doc = Mongo::Crypt::KMS::MasterKeyDocument.new('aws:name1', master_key).to_document
          expect(doc[:provider]).to eq('aws:name1')
          expect(doc[:region]).to eq('us-east-1')
        end
      end
    end
  end
end
