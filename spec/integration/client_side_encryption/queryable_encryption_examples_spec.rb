# frozen_string_literal: true

require 'spec_helper'

# No need to rewrite existing specs to make the examples shorter, until/unless
# we revisit these specs and need to make substantial changes.
# rubocop:disable RSpec/ExampleLength
describe 'Queryable encryption examples' do
  require_libmongocrypt
  min_server_version '7.0.0-rc0'
  require_topology :replica_set, :sharded, :load_balanced
  require_enterprise

  include_context 'define shared FLE helpers'

  it 'uses queryable encryption' do
    #  Drop data from prior test runs.
    authorized_client.use('docs_examples').database.drop
    authorized_client.use('keyvault')['datakeys'].drop

    # Create two data keys.
    # Note for docs team: remove the test_options argument when copying
    # this example into public documentation.
    key_vault_client = ClientRegistry.instance.new_local_client(
      SpecConfig.instance.addresses,
      SpecConfig.instance.test_options
    )
    client_encryption = Mongo::ClientEncryption.new(
      key_vault_client,
      key_vault_namespace: 'keyvault.datakeys',
      kms_providers: {
        local: {
          key: local_master_key
        }
      }
    )
    data_key_1_id = client_encryption.create_data_key('local')
    data_key_2_id = client_encryption.create_data_key('local')

    # Create an encryptedFieldsMap.
    encrypted_fields_map = {
      'docs_examples.encrypted' => {
        fields: [
          {
            path: 'encrypted_indexed',
            bsonType: 'string',
            keyId: data_key_1_id,
            queries: {
              queryType: 'equality'
            }
          },
          {
            path: 'encrypted_unindexed',
            bsonType: 'string',
            keyId: data_key_2_id,
          }
        ]
      }
    }

    # Create client with automatic queryable encryption enabled.
    # Note for docs team: remove the test_options argument when copying
    # this example into public documentation.
    encrypted_client = ClientRegistry.instance.new_local_client(
      SpecConfig.instance.addresses,
      SpecConfig.instance.test_options.merge(
        auto_encryption_options: {
          key_vault_namespace: 'keyvault.datakeys',
          kms_providers: {
            local: {
              key: local_master_key
            }
          },
          encrypted_fields_map: encrypted_fields_map,
          # Spawn mongocryptd on non-default port for sharded cluster tests
          # Note for docs team: remove the extra_options argument when copying
          # this example into public documentation.
          extra_options: extra_options,
        },
        database: 'docs_examples'
      )
    )
    # Create collection with queryable encryption enabled.
    encrypted_client['encrypted'].create

    # Auto encrypt an insert and find.
    encrypted_client['encrypted'].insert_one(
      _id: 1,
      encrypted_indexed: 'indexed_value',
      encrypted_unindexed: 'unindexed_value'
    )

    find_results = encrypted_client['encrypted'].find(
      encrypted_indexed: 'indexed_value'
    ).to_a
    expect(find_results.size).to eq(1)
    expect(find_results.first[:encrypted_indexed]).to eq('indexed_value')
    expect(find_results.first[:encrypted_unindexed]).to eq('unindexed_value')

    # Find documents without decryption.
    find_results = authorized_client
                   .use('docs_examples')['encrypted']
                   .find(_id: 1)
                   .to_a
    expect(find_results.size).to eq(1)
    expect(find_results.first[:encrypted_indexed]).to be_a(BSON::Binary)
    expect(find_results.first[:encrypted_unindexed]).to be_a(BSON::Binary)

    # Cleanup
    authorized_client.use('docs_examples').database.drop
    authorized_client.use('keyvault')['datakeys'].drop
  end
end
# rubocop:enable RSpec/ExampleLength
