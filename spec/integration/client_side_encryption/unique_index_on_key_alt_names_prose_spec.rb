# frozen_string_literal: true
# encoding: utf-8

require "spec_helper"

describe "Decryption events" do
  require_enterprise
  min_server_fcv "4.2"
  require_libmongocrypt
  include_context "define shared FLE helpers"

  let(:client) do
    ClientRegistry.instance.new_local_client(
      SpecConfig.instance.addresses,
      SpecConfig.instance.test_options.merge(
        database: SpecConfig.instance.test_db,
      )
    )
  end

  let(:client_encryption) do
    Mongo::ClientEncryption.new(
      client,
      key_vault_namespace: "#{key_vault_db}.#{key_vault_coll}",
      kms_providers: local_kms_providers
    )
  end

  let(:existing_key_alt_name) do
    'def'
  end

  let(:existing_key_id) do
    client_encryption.create_data_key('local', key_alt_names: [existing_key_alt_name])
  end

  before(:each) do
    client.use(key_vault_db)[key_vault_coll].drop
    client.use(key_vault_db).command({
      createIndexes: key_vault_coll,
      indexes: [
        {
          name: "keyAltNames_1",
          key: { "keyAltNames": 1 },
          unique: true,
          partialFilterExpression: { keyAltNames: { "$exists" => true } },
        },
      ],
      writeConcern: { w: "majority" },
    })
    # Force key creation
    existing_key_id
  end

  it 'tests create_data_key' do
    expect do
      client_encryption.create_data_key('local', key_alt_names: ['abc'])
    end.not_to raise_error

    expect do
      client_encryption.create_data_key('local', key_alt_names: [existing_key_alt_name])
    end.to raise_error(Mongo::Error::OperationFailure, /E11000/) # duplicate key error
  end

  it 'tests add_key_alt_name' do
    key_id = client_encryption.create_data_key('local')
    expect do
      client_encryption.add_key_alt_name(key_id, 'abc')
    end.not_to raise_error

    expect do
      key_document = client_encryption.add_key_alt_name(key_id, 'abc')
      expect(key_document['keyAltNames']).to include('abc')
    end.not_to raise_error

    expect do
      client_encryption.add_key_alt_name(key_id, existing_key_alt_name)
    end.to raise_error(Mongo::Error::OperationFailure, /E11000/) # duplicate key error

    expect do
      key_document = client_encryption.add_key_alt_name(existing_key_id, existing_key_alt_name)
      expect(key_document['keyAltNames']).to include(existing_key_alt_name)
    end.not_to raise_error
  end
end
