# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'Client-Side Encryption' do
  describe 'Prose tests: Corpus Test' do
    require_libmongocrypt
    require_enterprise
    min_server_fcv '4.2'

    include_context 'define shared FLE helpers'

    let(:client) { authorized_client }

    let(:key_vault_client) do
      client.with(
        database: 'keyvault',
        write_concern: { w: :majority }
      )['datakeys']
    end

    let(:test_schema_map) { BSON::ExtJSON.parse(File.read('spec/support/crypt/corpus/corpus-schema.json')) }
    let(:local_data_key) { BSON::ExtJSON.parse(File.read('spec/support/crypt/corpus/corpus-key-local.json')) }
    let(:aws_data_key) { BSON::ExtJSON.parse(File.read('spec/support/crypt/corpus/corpus-key-aws.json')) }
    let(:azure_data_key) { BSON::ExtJSON.parse(File.read('spec/support/crypt/corpus/corpus-key-azure.json')) }
    let(:gcp_data_key) { BSON::ExtJSON.parse(File.read('spec/support/crypt/corpus/corpus-key-gcp.json')) }
    let(:kmip_data_key) { BSON::ExtJSON.parse(File.read('spec/support/crypt/corpus/corpus-key-kmip.json')) }

    let(:client_encrypted) do
      new_local_client(
        SpecConfig.instance.addresses,
        SpecConfig.instance.test_options.merge(
          auto_encryption_options: {
            kms_providers: {
              local: { key: local_master_key },
              aws: {
                access_key_id: SpecConfig.instance.fle_aws_key,
                secret_access_key: SpecConfig.instance.fle_aws_secret,
              },
              azure: {
                tenant_id: SpecConfig.instance.fle_azure_tenant_id,
                client_id: SpecConfig.instance.fle_azure_client_id,
                client_secret: SpecConfig.instance.fle_azure_client_secret,
              },
              gcp: {
                email: SpecConfig.instance.fle_gcp_email,
                private_key: SpecConfig.instance.fle_gcp_private_key,
              },
              kmip: {
                endpoint: SpecConfig.instance.fle_kmip_endpoint,
              }
            },
            kms_tls_options: {
              kmip: {
                ssl_ca_cert: SpecConfig.instance.fle_kmip_tls_ca_file,
                ssl_cert: SpecConfig.instance.fle_kmip_tls_certificate_key_file,
                ssl_key: SpecConfig.instance.fle_kmip_tls_certificate_key_file,
              }
            },
            key_vault_namespace: 'keyvault.datakeys',
            schema_map: local_schema_map,
            # Spawn mongocryptd on non-default port for sharded cluster tests
            extra_options: extra_options,
          },
          database: 'db',
        )
      )
    end

    let(:client_encryption) do
      Mongo::ClientEncryption.new(
        client,
        {
          kms_providers: {
            local: { key: local_master_key },
            aws: {
              access_key_id: SpecConfig.instance.fle_aws_key,
              secret_access_key: SpecConfig.instance.fle_aws_secret,
            },
            azure: {
              tenant_id: SpecConfig.instance.fle_azure_tenant_id,
              client_id: SpecConfig.instance.fle_azure_client_id,
              client_secret: SpecConfig.instance.fle_azure_client_secret,
            },
            gcp: {
              email: SpecConfig.instance.fle_gcp_email,
              private_key: SpecConfig.instance.fle_gcp_private_key,
            },
            kmip: {
              endpoint: SpecConfig.instance.fle_kmip_endpoint,
            }
          },
          kms_tls_options: {
            kmip: {
              ssl_ca_cert: SpecConfig.instance.fle_kmip_tls_ca_file,
              ssl_cert: SpecConfig.instance.fle_kmip_tls_certificate_key_file,
              ssl_key: SpecConfig.instance.fle_kmip_tls_certificate_key_file,
            }
          },
          key_vault_namespace: 'keyvault.datakeys',
        },
      )
    end

    let(:corpus) do
      BSON::ExtJSON.parse(File.read('spec/support/crypt/corpus/corpus.json'), mode: :bson)
    end

    let(:corpus_encrypted_expected) do
      BSON::ExtJSON.parse(File.read('spec/support/crypt/corpus/corpus-encrypted.json'))
    end

    let(:corpus_copied) do
      # As per the instructions of the prose spec, corpus_copied is a copy of
      # the corpus BSON::Document that encrypts all fields that are meant to
      # be explicitly encrypted. corpus is a document containing many
      # sub-documents, each with a value to encrypt and information about how
      # to encrypt that value.
      corpus_copied = BSON::Document.new
      corpus.each do |key, doc|
        if ['_id', 'altname_aws', 'altname_azure', 'altname_gcp', 'altname_kmip', 'altname_local'].include?(key)
          corpus_copied[key] = doc
          next
        end

        if doc['method'] == 'auto'
          corpus_copied[key] = doc
        elsif doc['method'] == 'explicit'
          options = if doc['identifier'] == 'id'
            key_id = if doc['kms'] == 'local'
              'LOCALAAAAAAAAAAAAAAAAA=='
            elsif doc['kms'] == 'azure'
              'AZUREAAAAAAAAAAAAAAAAA=='
            elsif doc['kms'] == 'gcp'
              'GCPAAAAAAAAAAAAAAAAAAA=='
            elsif doc['kms'] == 'aws'
              'AWSAAAAAAAAAAAAAAAAAAA=='
            elsif doc['kms'] == 'kmip'
              'KMIPAAAAAAAAAAAAAAAAAA=='
            end

            { key_id: BSON::Binary.new(Base64.decode64(key_id), :uuid) }
          elsif doc['identifier'] == 'altname'
            { key_alt_name: doc['kms'] }
          end

          algorithm = if doc['algo'] == 'rand'
            'AEAD_AES_256_CBC_HMAC_SHA_512-Random'
          else
            'AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic'
          end

          begin
            encrypted_value = client_encryption.encrypt(
              doc['value'],
              options.merge({ algorithm: algorithm })
            )

            corpus_copied[key] = doc.merge('value' => encrypted_value)
          rescue => e
            # If doc['allowed'] is true, it means that this field should have
            # been encrypted without error, and thus that this error is unexpected.
            # If doc['allowed'] is false, this error was expected and the value
            # should be copied over without being encrypted.
            if doc['allowed']
              raise "Unexpected error occurred in client-side encryption " +
                "corpus tests: #{e.class}: #{e.message}"
            end

            corpus_copied[key] = doc
          end
        end
      end

      corpus_copied
    end

    before do
      client.use('db')['coll'].drop

      key_vault_collection = client.use('keyvault')['datakeys', write_concern: { w: :majority }]
      key_vault_collection.drop
      key_vault_collection.insert_one(local_data_key)
      key_vault_collection.insert_one(aws_data_key)
      key_vault_collection.insert_one(azure_data_key)
      key_vault_collection.insert_one(gcp_data_key)
      key_vault_collection.insert_one(kmip_data_key)
    end

    shared_context 'with jsonSchema collection validator' do
      let(:local_schema_map) { nil }

      before do
        client.use('db')['coll',
          {
            'validator' => { '$jsonSchema' => test_schema_map }
          }
        ].create
      end
    end

    shared_context 'with local schema map' do
      let(:local_schema_map) { { 'db.coll' => test_schema_map } }
    end

    shared_examples 'a functioning encrypter' do
      it 'properly encrypts and decrypts a document' do
        corpus_encrypted_id = client_encrypted['coll']
          .insert_one(corpus_copied)
          .inserted_id

        corpus_decrypted = client_encrypted['coll']
          .find(_id: corpus_encrypted_id)
          .first

        # Ensure that corpus_decrypted is the same as the original corpus
        # document by checking that they have the same set of keys, and that
        # they have the same values at those keys (improved diagnostics).
        expect(corpus_decrypted.keys).to eq(corpus.keys)

        corpus_decrypted.each do |key, doc|
          expect(key => doc).to eq(key => corpus[key])
        end

        corpus_encrypted_actual = client
          .use('db')['coll']
          .find(_id: corpus_encrypted_id)
          .first


        corpus_encrypted_actual.each do |key, value|
          # If it was deterministically encrypted, test the encrypted values
          # for equality.
          if value['algo'] == 'det'
            expect(value['value']).to eq(corpus_encrypted_expected[key]['value'])
          else
            # If the document was randomly encrypted, the two encrypted values
            # will not be equal. Ensure that they are equal when decrypted.
            if value['allowed']
              actual_decrypted_value = client_encryption.decrypt(value['value'])
              expected_decrypted_value = client_encryption.decrypt(corpus_encrypted_expected[key]['value'])

              expect(actual_decrypted_value).to eq(expected_decrypted_value)
            else
              # If 'allowed' was false, the value was never encrypted; ensure
              # that it is equal to the original, unencrypted value.
              expect(value['value']).to eq(corpus[key]['value'])
            end
          end
        end
      end
    end

    context 'with collection validator' do
      include_context 'with jsonSchema collection validator'
      it_behaves_like 'a functioning encrypter'
    end

    context 'with schema map' do
      include_context 'with local schema map'
      it_behaves_like 'a functioning encrypter'
    end
  end
end
