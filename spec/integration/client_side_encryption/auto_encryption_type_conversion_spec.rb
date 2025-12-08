# frozen_string_literal: true

require 'spec_helper'

describe 'Auto Encryption Type Conversion' do
  require_libmongocrypt
  require_enterprise
  min_server_fcv '4.2'

  include_context 'define shared FLE helpers'
  include_context 'with local kms_providers'

  let(:client) do
    new_local_client(
      SpecConfig.instance.addresses,
      SpecConfig.instance.test_options.merge(
        auto_encryption_options: {
          kms_providers: kms_providers,
          key_vault_namespace: key_vault_namespace,
          schema_map: { 'auto_encryption.users' => schema_map },
          # Spawn mongocryptd on non-default port for sharded cluster tests
          extra_options: extra_options,
        },
        database: 'auto_encryption'
      )
    )
  end

  let(:large_number) { 2**40 }

  let(:ssn) { '123-45-6789' }

  let(:decrypted_doc) do
    client['users'].find(_id: 1).first
  end

  before do
    authorized_client.use('auto_encryption')['users'].drop

    key_vault_collection.drop
    key_vault_collection.insert_one(data_key)

    client['users'].insert_one(
      _id: 1,
      large_number: large_number,
      ssn: ssn
    )
  end

  context 'when csfle_convert_to_ruby_types is true' do
    config_override :csfle_convert_to_ruby_types, true

    it 'returns Int64 as Integer' do
      expect(decrypted_doc['ssn']).to eq(ssn) # To check that decryption works
      expect(decrypted_doc['large_number']).to be_a(Integer)
      expect(decrypted_doc['large_number']).to eq(large_number)
    end
  end

  context 'when csfle_convert_to_ruby_types is false' do
    config_override :csfle_convert_to_ruby_types, false

    it 'returns Int64 as BSON::Int64' do
      expect(decrypted_doc['ssn']).to eq(ssn) # To check that decryption works
      expect(decrypted_doc['large_number']).to be_a(BSON::Int64)
      expect(decrypted_doc['large_number'].value).to eq(large_number)
    end
  end
end
