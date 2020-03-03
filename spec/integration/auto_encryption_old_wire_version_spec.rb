require 'spec_helper'

describe 'Auto Encryption' do
  require_libmongocrypt
  max_server_version '4.0'

  # Diagnostics of leaked background threads only, these tests do not
  # actually require a clean slate. https://jira.mongodb.org/browse/RUBY-2138
  clean_slate

  include_context 'define shared FLE helpers'

  let(:encryption_client) do
    new_local_client(
      SpecConfig.instance.addresses,
      SpecConfig.instance.test_options.merge(
        auto_encryption_options: {
          kms_providers: kms_providers,
          key_vault_namespace: key_vault_namespace,
          # Must use local schema map because server versions older than 4.2
          # do not support jsonSchema collection validator.
          schema_map: { 'auto_encryption.users' => schema_map },
          bypass_auto_encryption: bypass_auto_encryption
        },
        database: 'auto_encryption'
      ),
    )
  end

  let(:bypass_auto_encryption) { false }
  let(:client) { authorized_client.use(:auto_encryption) }

  let(:encrypted_ssn_binary) do
    BSON::Binary.new(Base64.decode64(encrypted_ssn), :ciphertext)
  end

  shared_examples 'it decrypts but does not encrypt on wire version < 8' do
    before do
      client[:users].insert_one(ssn: encrypted_ssn_binary)

      authorized_client.use(:admin)[:datakeys].drop
      authorized_client.use(:admin)[:datakeys].insert_one(data_key)
    end

    it 'raises an exception when trying to encrypt' do
      expect do
        encryption_client[:users].find(ssn: ssn).first
      end.to raise_error(Mongo::Error::CryptError, /Auto-encryption requires a minimum MongoDB version of 4.2/)
    end

    context 'with bypass_auto_encryption=true' do
      let(:bypass_auto_encryption) { true }

      it 'does not raise an exception but doesn\'t encrypt' do
        document = encryption_client[:users].find(ssn: ssn).first
        expect(document).to be_nil
      end

      it 'still decrypts' do
        document = encryption_client[:users].find(ssn: encrypted_ssn_binary).first
        # ssn field is still decrypted
        expect(document['ssn']).to eq(ssn)
      end
    end
  end

  context 'with AWS kms provider' do
    include_context 'with AWS kms_providers'
    it_behaves_like 'it decrypts but does not encrypt on wire version < 8'
  end

  context 'with local kms provider' do
    include_context 'with local kms_providers'
    it_behaves_like 'it decrypts but does not encrypt on wire version < 8'
  end
end
