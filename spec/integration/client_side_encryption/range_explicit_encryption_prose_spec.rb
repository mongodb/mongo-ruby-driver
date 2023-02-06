# frozen_string_literal: true

require 'spec_helper'

describe 'Range Explicit Encryption' do
  require_libmongocrypt
  include_context 'define shared FLE helpers'

  let(:key1_id) do
    key1_document['_id']
  end

  before(:each) do
    authorized_client['explicit_encryption'].drop(encrypted_fields: encrypted_fields)
    authorized_client['explicit_encryption'].create(encrypted_fields: encrypted_fields)
    authorized_client.use(key_vault_db)[key_vault_coll].drop
    authorized_client.use(key_vault_db)[key_vault_coll, write_concern: {w: :majority}].insert_one(key1_document)
  end

  let(:key_vault_client) do
    ClientRegistry.instance.new_local_client(SpecConfig.instance.addresses)
  end

  let(:client_encryption) do
    Mongo::ClientEncryption.new(
      key_vault_client,
      kms_tls_options: kms_tls_options,
      key_vault_namespace: key_vault_namespace,
      kms_providers: local_kms_providers
    )
  end

  let(:encrypted_client) do
    ClientRegistry.instance.new_local_client(
      SpecConfig.instance.addresses,
      auto_encryption_options: {
        key_vault_namespace: key_vault_namespace,
        kms_providers: local_kms_providers,
        bypass_query_analysis: true
      },
      database: SpecConfig.instance.test_db
    )
  end

  context 'Int' do
    let(:encrypted_fields) do
      range_encrypted_fields_int
    end

    let(:range_opts) do
      {
        min: BSON::Int32.new(0),
        max: BSON::Int32.new(200),
        sparsity: 1
      }
    end

    it 'can decrypt a payload' do
      insert_payload = client_encryption.encrypt_expression(6, range_opts: range_opts)
    end
  end
end
