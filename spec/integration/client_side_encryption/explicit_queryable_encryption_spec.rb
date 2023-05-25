# frozen_string_literal: true

require 'spec_helper'

# No need to rewrite existing specs to make the examples shorter, until/unless
# we revisit these specs and need to make substantial changes.
# rubocop:disable RSpec/ExampleLength
describe 'Explicit Queryable Encryption' do
  require_libmongocrypt
  min_server_version '7.0.0-rc0'
  require_topology :replica_set, :sharded, :load_balanced

  include_context 'define shared FLE helpers'

  let(:key1_id) do
    key1_document['_id']
  end

  let(:encrypted_coll) do
    'explicit_encryption'
  end

  let(:value) do
    'encrypted indexed value'
  end

  let(:unindexed_value) do
    'encrypted unindexed value'
  end

  let(:key_vault_client) do
    ClientRegistry.instance.new_local_client(SpecConfig.instance.addresses)
  end

  let(:client_encryption_opts) do
    {
      kms_providers: local_kms_providers,
      kms_tls_options: kms_tls_options,
      key_vault_namespace: key_vault_namespace
    }
  end

  let(:client_encryption) do
    Mongo::ClientEncryption.new(
      key_vault_client,
      client_encryption_opts
    )
  end

  let(:encrypted_client) do
    ClientRegistry.instance.new_local_client(
      SpecConfig.instance.addresses,
      auto_encryption_options: {
        key_vault_namespace: "#{key_vault_db}.#{key_vault_coll}",
        kms_providers: local_kms_providers,
        bypass_query_analysis: true
      },
      database: SpecConfig.instance.test_db
    )
  end

  before do
    authorized_client[encrypted_coll].drop(encrypted_fields: encrypted_fields)
    authorized_client[encrypted_coll].create(encrypted_fields: encrypted_fields)
    authorized_client.use(key_vault_db)[key_vault_coll].drop
    authorized_client.use(key_vault_db)[key_vault_coll, write_concern: { w: :majority }].insert_one(key1_document)
  end

  after do
    authorized_client[encrypted_coll].drop(encrypted_fields: encrypted_fields)
    authorized_client.use(key_vault_db)[key_vault_coll].drop
  end

  it 'can insert encrypted indexed and find' do
    insert_payload = client_encryption.encrypt(
      value, key_id: key1_id, algorithm: 'Indexed', contention_factor: 0
    )
    encrypted_client[encrypted_coll].insert_one(
      'encryptedIndexed' => insert_payload
    )
    find_payload = client_encryption.encrypt(
      value, key_id: key1_id, algorithm: 'Indexed', query_type: 'equality', contention_factor: 0
    )
    find_results = encrypted_client[encrypted_coll]
                   .find('encryptedIndexed' => find_payload)
                   .to_a
    expect(find_results.size).to eq(1)
    expect(find_results.first['encryptedIndexed']).to eq(value)
  end

  it 'can insert encrypted indexed and find with non-zero contention' do
    10.times do
      insert_payload = client_encryption.encrypt(
        value, key_id: key1_id, algorithm: 'Indexed', contention_factor: 10
      )
      encrypted_client[encrypted_coll].insert_one(
        'encryptedIndexed' => insert_payload
      )
    end
    find_payload = client_encryption.encrypt(
      value, key_id: key1_id, algorithm: 'Indexed', query_type: 'equality', contention_factor: 0
    )
    find_results = encrypted_client[encrypted_coll]
                   .find('encryptedIndexed' => find_payload)
                   .to_a
    expect(find_results.size).to be < 10
    find_results.each do |doc|
      expect(doc['encryptedIndexed']).to eq(value)
    end
    find_payload2 = client_encryption.encrypt(
      value, key_id: key1_id, algorithm: 'Indexed', query_type: 'equality', contention_factor: 10
    )
    find_results2 = encrypted_client[encrypted_coll]
                    .find('encryptedIndexed' => find_payload2)
                    .to_a
    expect(find_results2.size).to eq(10)
    find_results2.each do |doc|
      expect(doc['encryptedIndexed']).to eq(value)
    end
  end

  it 'can insert encrypted unindexed' do
    insert_payload = client_encryption.encrypt(
      unindexed_value, key_id: key1_id, algorithm: 'Unindexed'
    )
    encrypted_client[encrypted_coll].insert_one(
      '_id' => 1, 'encryptedUnindexed' => insert_payload
    )
    find_results = encrypted_client[encrypted_coll].find('_id' => 1).to_a
    expect(find_results.size).to eq(1)
    expect(find_results.first['encryptedUnindexed']).to eq(unindexed_value)
  end

  it 'can roundtrip encrypted indexed' do
    payload = client_encryption.encrypt(
      value, key_id: key1_id, algorithm: 'Indexed', contention_factor: 0
    )
    decrypted_value = client_encryption.decrypt(payload)
    expect(decrypted_value).to eq(value)
  end

  it 'can roundtrip encrypted unindexed' do
    payload = client_encryption.encrypt(
      unindexed_value, key_id: key1_id, algorithm: 'Unindexed'
    )
    decrypted_value = client_encryption.decrypt(payload)
    expect(decrypted_value).to eq(unindexed_value)
  end
end
# rubocop:enable RSpec/ExampleLength
