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

    let(:test_schema_map) { BSON::ExtJSON.parse(File.read('spec/support/crypt/corpus/corpus-schema.json'), mode: :bson) }
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
            schema_map: local_schema_map,
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
      BSON::ExtJSON.parse(File.read('spec/support/crypt/corpus/corpus.json'), mode: :bson)
    end

    let(:corpus_encrypted_expected) do
      BSON::ExtJSON.parse(File.read('spec/support/crypt/corpus/corpus_encrypted.json'), mode: :bson)
    end

    let(:corpus_copied) do
      doc = BSON::Document.new
      corpus.each do |key, value|
        if ['_id', 'altname_aws', 'altname_local'].include?(key)
          doc[key] = value
          next
        end

        if value['method'] == 'auto'
          doc[key] = value
        elsif value['method'] == 'explicit'
          options = {}

          options = if value['identifier'] == 'id'
            key_id = if value['kms'] == 'local'
              BSON::Binary.new(Base64.decode64('LOCALAAAAAAAAAAAAAAAAA=='), :uuid)
            else
              BSON::Binary.new(Base64.decode64('AWSAAAAAAAAAAAAAAAAAAA=='), :uuid)
            end

            { key_id: key_id }
          elsif value['identifier'] == 'altname'
            { key_alt_name: value['kms'] }
          end

          algorithm = if value['algo'] == 'rand'
            'AEAD_AES_256_CBC_HMAC_SHA_512-Random'
          else
            'AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic'
          end

          begin
            encrypted_value = client_encryption.encrypt(
              value['value'],
              options.merge({ algorithm: algorithm })
            )

            doc[key] = value.merge('value' => encrypted_value)
          rescue => e
            if value['allowed']
              raise "Unexpected error occured in client-side encryption " +
                "corpus tests: #{e.class}, #{e.message}"
            end

            doc[key] = value
          end
        end
      end

      doc
    end

    before do
      client.use(:db)[:coll].drop

      client.use(:admin)[:datakeys].drop
      client.use(:admin)[:datakeys].insert_one(local_data_key)
      client.use(:admin)[:datakeys].insert_one(aws_data_key)
    end

    shared_context 'with jsonSchema collection validator' do
      let(:local_schema_map) { nil }

      before do
        client.use(:db)[:coll,
          {
            'validator' => { '$jsonSchema' => test_schema_map }
          }
        ].create
      end
    end

    shared_context 'with local schema map' do
      let(:local_schema_map) { { 'db.coll' => test_schema_map } }
    end

    shared_examples 'something' do
      it 'does a thing' do
        result = client_encrypted[:coll].insert_one(corpus_copied)
        corpus_decrypted = client_encrypted[:coll].find(_id: result.inserted_id).first

        corpus_decrypted.each do |key, value|
          next if value['value'].is_a?(Time) # TODO: deal with this
          expect(value).to eq(corpus[key])
        end
        # expect(corpus_decrypted).to eq(corpus)
        corpus_encrypted_actual = client.use(:db)[:coll].find(_id: result.inserted_id).first

        corpus_encrypted_expected.each do |key, value|
          if value['algo'] == 'det'
            expect(value['value']).to eq(corpus_encrypted_actual[key]['value'])
          elsif value['algo'] == 'rand' && value['allowed']
            expect(value['value']).not_to eq(corpus_encrypted_actual[key]['value'])
          elsif !value['allowed']
            expect(value['value']).to eq(corpus[key]['value'])
          end
        end
      end
    end

    context 'with local KMS provider' do
      include_context 'with local kms_providers'

      context 'with collection validator' do
        include_context 'with jsonSchema collection validator'
        it_behaves_like 'something'
      end

      context 'with schema map' do
        include_context 'with local schema map'
        it_behaves_like 'something'
      end
    end

    context 'with AWS KMS provider' do
      include_context 'with AWS kms_providers'

      context 'with collection validator' do
        include_context 'with jsonSchema collection validator'
        it_behaves_like 'something'
      end

      context 'with schema map' do
        include_context 'with local schema map'
        it_behaves_like 'something'
      end
    end
  end
end
