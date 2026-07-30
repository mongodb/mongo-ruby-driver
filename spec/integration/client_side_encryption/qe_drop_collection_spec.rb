# frozen_string_literal: true

require 'spec_helper'

describe 'Queryable encryption drop collection' do
  require_libmongocrypt
  min_server_version '7.0.0-rc0'
  require_topology :replica_set, :sharded, :load_balanced

  include_context 'define shared FLE helpers'

  let(:encrypted_coll) { 'qe_drop_lookup' }

  # The shared fixture hardcodes metadata collection names for a collection
  # named "default"; remove them so the default enxcol_.<name>.esc/.ecoc
  # names for the collection under test apply.
  let(:encrypted_fields) do
    BSON::ExtJSON.parse(
      File.read('spec/support/crypt/encrypted_fields/encryptedFields.json')
    ).tap do |fields|
      fields.delete('escCollection')
      fields.delete('ecocCollection')
    end
  end

  let(:auto_encryption_options) do
    {
      key_vault_namespace: key_vault_namespace,
      kms_providers: local_kms_providers,
      bypass_query_analysis: true
    }
  end

  let(:encrypted_client) do
    ClientRegistry.instance.new_local_client(
      SpecConfig.instance.addresses,
      auto_encryption_options: auto_encryption_options,
      database: SpecConfig.instance.test_db
    )
  end

  before do
    authorized_client[encrypted_coll].drop(encrypted_fields: encrypted_fields)
    authorized_client[encrypted_coll].create(encrypted_fields: encrypted_fields)
  end

  after do
    authorized_client[encrypted_coll].drop(encrypted_fields: encrypted_fields)
  end

  shared_examples 'drops the metadata collections' do
    it 'looks up encryptedFields on the server and drops the metadata collections' do
      expect(authorized_client.database.collection_names)
        .to include("enxcol_.#{encrypted_coll}.esc", "enxcol_.#{encrypted_coll}.ecoc")

      encrypted_client[encrypted_coll].drop

      collection_names = authorized_client.database.collection_names
      expect(collection_names).not_to include("enxcol_.#{encrypted_coll}.esc")
      expect(collection_names).not_to include("enxcol_.#{encrypted_coll}.ecoc")
      expect(collection_names).not_to include(encrypted_coll)
    end
  end

  context 'when auto encryption is configured without encrypted_fields_map' do
    include_examples 'drops the metadata collections'
  end

  context 'when encrypted_fields_map has no entry for the collection' do
    let(:auto_encryption_options) do
      super().merge(encrypted_fields_map: {})
    end

    include_examples 'drops the metadata collections'
  end
end
