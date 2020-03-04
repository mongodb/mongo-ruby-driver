require 'spec_helper'

describe 'Client-Side Encryption' do
  describe 'Prose tests: Corpus Test' do
    require_libmongocrypt
    include_context 'define shared FLE helpers'

    let(:client) do
      new_local_client(
        SpecConfig.instance.addresses,
        SpecConfig.instance.test_options
      )
    end

    let(:schema_map) { BSON::ExtJSON.parse(File.read('spec/support/crypt/corpus/corpus-schema.json')) }
    let(:local_data_key) { BSON::ExtJSON.parse(File.read('spec/support/crypt/corpus/corpus-key-local.json')) }
    let(:aws_data_key) { BSON::ExtJSON.parse(File.read('spec/support/crypt/corpus/corpus-key-aws.json')) }

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
            },
            key_vault_namespace: 'admin.datakeys',
            schema_map: test_schema_map,
          },
          database: :db,
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
            }
          },
          key_vault_namespace: 'admin.datakeys',
        },
      )
    end

    let(:corpus) do
      BSON::ExtJSON.parse(File.read('spec/support/crypt/corpus/corpus.json'))
    end

    let(:corpus_encrypted_expected) do
      BSON::ExtJSON.parse(File.read('spec/support/crypt/corpus/corpus_encrypted.json'))
    end

    let(:corpus_copied) do
      doc = BSON::Document.new
      corpus.each do |key, value|
        if ['_id', 'altname_aws', 'altname_local'].include?(key)
          doc[key] = value
        end

        if value['method'] == 'auto'
          doc[key] = value
        end

        if value['method'] == 'explicit'
          options = {}

          options = if value['identifier'] == 'id'
            {

            }
          elsif value['identifier'] == 'altname'
            {
              key_alt_name: value['kms']
            }
          end

          begin
            doc[key] = client_encryption.encrypt(
              value['value'],
              {
                key_id: key_id,
                algorithm: value['algo']
              }
            )
          rescue => e
            if value['allowed']
              raise "Unexpected error occured in client-side encryption " +
                "corpus tests: #{e.class}, #{e.message}"
            end

            doc[key] = value['value']
          end
        end
      end

      doc
    end

    before do
      client.use(:db)[:coll].drop
      client.use(:db)[:coll,
        {
          'validator' => { '$jsonSchema' => schema_map }
        }
      ].create

      client.use(:admin)[:datakeys].drop
      client.use(:admin)[:datakeys].insert_one(local_data_key)
      client.use(:admin)[:datakeys].insert_one(aws_data_key)
    end

    shared_examples 'something' do
      it 'does a thing' do
        result = client_encrypted[:coll].insert_one(corpus_copied)
        corpus_decrypted = client_encrypted[:coll].find(_id: result.inserted_id).first
        expect(corpus_decrypted).to eq(corpus)
      end
    end
  end
end
