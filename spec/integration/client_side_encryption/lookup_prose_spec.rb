# frozen_string_literal: true

require 'spec_helper'

describe 'Prose Test 25: $lookup support with CSFLE and QE' do
  require_libmongocrypt
  require_enterprise
  require_topology :replica_set
  min_server_version '7.0'

  include_context 'define shared FLE helpers'

  let(:local_master_key) { Base64.decode64(Crypt::LOCAL_MASTER_KEY_B64) }

  let(:auto_encryption_options) do
    {
      key_vault_namespace: 'db.keyvault',
      kms_providers: { local: { key: local_master_key } },
      extra_options: {
        mongocryptd_spawn_args: [ "--port=#{SpecConfig.instance.mongocryptd_port}" ],
        mongocryptd_uri: "mongodb://localhost:#{SpecConfig.instance.mongocryptd_port}",
      }
    }
  end

  def new_encrypted_client
    ClientRegistry.instance.new_local_client(
      SpecConfig.instance.addresses,
      SpecConfig.instance.test_options.merge(
        auto_encryption_options: auto_encryption_options,
        database: 'db'
      )
    )
  end

  before(:all) do
    key_doc = BSON::ExtJSON.parse(File.read('spec/support/crypt/lookup/key-doc.json'))
    schema_csfle = BSON::ExtJSON.parse(File.read('spec/support/crypt/lookup/schema-csfle.json'))
    schema_csfle2 = BSON::ExtJSON.parse(File.read('spec/support/crypt/lookup/schema-csfle2.json'))
    schema_qe = BSON::ExtJSON.parse(File.read('spec/support/crypt/lookup/schema-qe.json'))
    schema_qe2 = BSON::ExtJSON.parse(File.read('spec/support/crypt/lookup/schema-qe2.json'))
    schema_non_csfle = BSON::ExtJSON.parse(File.read('spec/support/crypt/lookup/schema-non-csfle.json'))

    local_master_key = Base64.decode64(Crypt::LOCAL_MASTER_KEY_B64)

    setup_client = ClientRegistry.instance.new_local_client(
      SpecConfig.instance.addresses,
      SpecConfig.instance.test_options
    )

    # Set up key vault
    setup_client.use('db')['keyvault'].drop
    setup_client.use('db')['keyvault', write_concern: { w: :majority }].insert_one(key_doc)

    # Drop and recreate CSFLE collections
    setup_client.use('db')['csfle'].drop
    setup_client.use('db').command(
      create: 'csfle',
      validator: { '$jsonSchema' => schema_csfle }
    )

    setup_client.use('db')['csfle2'].drop
    setup_client.use('db').command(
      create: 'csfle2',
      validator: { '$jsonSchema' => schema_csfle2 }
    )

    # Drop and recreate QE collections
    begin
      setup_client.use('db').command(drop: 'qe')
    rescue StandardError
      nil
    end
    setup_client.use('db').command(create: 'qe', encryptedFields: schema_qe)

    begin
      setup_client.use('db').command(drop: 'qe2')
    rescue StandardError
      nil
    end
    setup_client.use('db').command(create: 'qe2', encryptedFields: schema_qe2)

    # Drop and recreate plain collections
    setup_client.use('db')['no_schema'].drop
    setup_client.use('db')['no_schema'].create

    setup_client.use('db')['no_schema2'].drop
    setup_client.use('db')['no_schema2'].create

    # Drop and recreate non-CSFLE schema collection
    setup_client.use('db')['non_csfle_schema'].drop
    setup_client.use('db').command(
      create: 'non_csfle_schema',
      validator: { '$jsonSchema' => schema_non_csfle }
    )

    # Insert test data using encrypted client (so fields get encrypted)
    encrypted_client = ClientRegistry.instance.new_local_client(
      SpecConfig.instance.addresses,
      SpecConfig.instance.test_options.merge(
        auto_encryption_options: {
          key_vault_namespace: 'db.keyvault',
          kms_providers: { local: { key: local_master_key } },
          extra_options: {
            mongocryptd_spawn_args: [ "--port=#{SpecConfig.instance.mongocryptd_port}" ],
            mongocryptd_uri: "mongodb://localhost:#{SpecConfig.instance.mongocryptd_port}",
          }
        },
        database: 'db'
      )
    )

    encrypted_client['csfle'].insert_one(csfle: 'csfle')
    encrypted_client['csfle2'].insert_one(csfle2: 'csfle2')
    encrypted_client['qe'].insert_one(qe: 'qe')
    encrypted_client['qe2'].insert_one(qe2: 'qe2')
    encrypted_client['no_schema'].insert_one(no_schema: 'no_schema')
    encrypted_client['no_schema2'].insert_one(no_schema2: 'no_schema2')
    encrypted_client['non_csfle_schema'].insert_one(non_csfle_schema: 'non_csfle_schema')

    # Verify csfle and qe fields are encrypted at rest
    csfle_doc = setup_client.use('db')['csfle'].find.first
    raise 'Expected csfle field to be encrypted' unless csfle_doc['csfle'].is_a?(BSON::Binary)

    qe_doc = setup_client.use('db')['qe'].find.first
    raise 'Expected qe field to be encrypted' unless qe_doc['qe'].is_a?(BSON::Binary)
  end

  def lookup_pipeline(from:, project_out: nil)
    inner_project = { '_id' => 0 }
    inner_project[project_out] = 0 if project_out

    [
      { '$match' => {} },
      {
        '$lookup' => {
          'from' => from,
          'pipeline' => [
            { '$match' => {} },
            { '$project' => inner_project }
          ],
          'as' => 'matched'
        }
      },
      { '$project' => { '_id' => 0 } }
    ]
  end

  def lookup_pipeline_with_outer_project(from:, outer_project_out:, inner_project_out: nil)
    inner_project = { '_id' => 0 }
    inner_project[inner_project_out] = 0 if inner_project_out

    outer_project = { '_id' => 0, outer_project_out => 0 }

    [
      { '$match' => {} },
      {
        '$lookup' => {
          'from' => from,
          'pipeline' => [
            { '$match' => {} },
            { '$project' => inner_project }
          ],
          'as' => 'matched'
        }
      },
      { '$project' => outer_project }
    ]
  end

  context 'Case 1: CSFLE collection $lookup from unencrypted collection' do
    min_server_version '8.1'

    it 'decrypts both collections correctly' do
      client = new_encrypted_client
      pipeline = lookup_pipeline(from: 'no_schema')
      result = client['csfle'].aggregate(pipeline).to_a

      expect(result.length).to eq(1)
      expect(result.first['csfle']).to eq('csfle')
      expect(result.first['matched']).to eq([ { 'no_schema' => 'no_schema' } ])
    end
  end

  context 'Case 2: QE collection $lookup from unencrypted collection' do
    min_server_version '8.1'

    it 'decrypts both collections correctly' do
      client = new_encrypted_client
      pipeline = lookup_pipeline_with_outer_project(from: 'no_schema', outer_project_out: '__safeContent__')
      result = client['qe'].aggregate(pipeline).to_a

      expect(result.length).to eq(1)
      expect(result.first['qe']).to eq('qe')
      expect(result.first['matched']).to eq([ { 'no_schema' => 'no_schema' } ])
    end
  end

  context 'Case 3: Unencrypted collection $lookup from CSFLE collection' do
    min_server_version '8.1'

    it 'decrypts the inner collection correctly' do
      client = new_encrypted_client
      pipeline = lookup_pipeline(from: 'csfle')
      result = client['no_schema'].aggregate(pipeline).to_a

      expect(result.length).to eq(1)
      expect(result.first['no_schema']).to eq('no_schema')
      expect(result.first['matched']).to eq([ { 'csfle' => 'csfle' } ])
    end
  end

  context 'Case 4: Unencrypted collection $lookup from QE collection' do
    min_server_version '8.1'

    it 'decrypts the inner collection correctly' do
      client = new_encrypted_client
      pipeline = lookup_pipeline(from: 'qe', project_out: '__safeContent__')
      result = client['no_schema'].aggregate(pipeline).to_a

      expect(result.length).to eq(1)
      expect(result.first['no_schema']).to eq('no_schema')
      expect(result.first['matched']).to eq([ { 'qe' => 'qe' } ])
    end
  end

  context 'Case 5: CSFLE collection $lookup from another CSFLE collection' do
    min_server_version '8.1'

    it 'decrypts both collections correctly' do
      client = new_encrypted_client
      pipeline = lookup_pipeline(from: 'csfle2')
      result = client['csfle'].aggregate(pipeline).to_a

      expect(result.length).to eq(1)
      expect(result.first['csfle']).to eq('csfle')
      expect(result.first['matched']).to eq([ { 'csfle2' => 'csfle2' } ])
    end
  end

  context 'Case 6: QE collection $lookup from another QE collection' do
    min_server_version '8.1'

    it 'decrypts both collections correctly' do
      client = new_encrypted_client
      pipeline = lookup_pipeline_with_outer_project(
        from: 'qe2',
        outer_project_out: '__safeContent__',
        inner_project_out: '__safeContent__'
      )
      result = client['qe'].aggregate(pipeline).to_a

      expect(result.length).to eq(1)
      expect(result.first['qe']).to eq('qe')
      expect(result.first['matched']).to eq([ { 'qe2' => 'qe2' } ])
    end
  end

  context 'Case 7: Unencrypted collection $lookup from another unencrypted collection' do
    min_server_version '8.1'

    it 'works without encryption' do
      client = new_encrypted_client
      pipeline = lookup_pipeline(from: 'no_schema2')
      result = client['no_schema'].aggregate(pipeline).to_a

      expect(result.length).to eq(1)
      expect(result.first['no_schema']).to eq('no_schema')
      expect(result.first['matched']).to eq([ { 'no_schema2' => 'no_schema2' } ])
    end
  end

  context 'Case 8: CSFLE collection $lookup from QE collection raises error' do
    min_server_version '8.1'

    it 'raises an error' do
      client = new_encrypted_client
      pipeline = lookup_pipeline(from: 'qe')
      expect do
        client['csfle'].aggregate(pipeline).to_a
      end.to raise_error(Mongo::Error, /not supported|Cannot specify both encryptionInformation/)
    end
  end

  context 'Case 9: CSFLE $lookup on server version < 8.1 raises upgrade error' do
    max_server_version '8.0.99'

    it 'raises an error suggesting upgrade' do
      client = new_encrypted_client
      pipeline = lookup_pipeline(from: 'no_schema')
      expect do
        client['csfle'].aggregate(pipeline).to_a
      end.to raise_error(Mongo::Error, /Upgrade/)
    end
  end

  context 'Case 10: QE collection $lookup from collection with non-CSFLE schema' do
    min_server_version '8.2'

    it 'decrypts both collections correctly' do
      client = new_encrypted_client
      pipeline = lookup_pipeline_with_outer_project(from: 'non_csfle_schema', outer_project_out: '__safeContent__')
      result = client['qe'].aggregate(pipeline).to_a

      expect(result.length).to eq(1)
      expect(result.first['qe']).to eq('qe')
      expect(result.first['matched']).to eq([ { 'non_csfle_schema' => 'non_csfle_schema' } ])
    end
  end
end
