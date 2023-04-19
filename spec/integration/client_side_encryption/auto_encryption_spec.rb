# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'
require 'bson'
require 'json'

describe 'Auto Encryption' do
  require_libmongocrypt
  min_server_fcv '4.2'
  require_enterprise

  # Diagnostics of leaked background threads only, these tests do not
  # actually require a clean slate. https://jira.mongodb.org/browse/RUBY-2138
  clean_slate

  include_context 'define shared FLE helpers'

  let(:encryption_client) do
    new_local_client(
      SpecConfig.instance.addresses,
      SpecConfig.instance.test_options.merge(
        auto_encryption_options: {
          kms_providers: kms_providers,
          kms_tls_options: kms_tls_options,
          key_vault_namespace: key_vault_namespace,
          schema_map: local_schema,
          bypass_auto_encryption: bypass_auto_encryption,
          # Spawn mongocryptd on non-default port for sharded cluster tests
          extra_options: extra_options,
        },
        database: 'auto_encryption',
        max_pool_size: max_pool_size
      ),
    )
  end

  let(:client) { authorized_client.use('auto_encryption') }

  let(:bypass_auto_encryption) { false }

  let(:max_pool_size) do
    Mongo::Server::ConnectionPool::DEFAULT_MAX_SIZE
  end

  let(:encrypted_ssn_binary) do
    BSON::Binary.new(Base64.decode64(encrypted_ssn), :ciphertext)
  end

  shared_context 'bypass auto encryption' do
    let(:bypass_auto_encryption) { true }
  end

  shared_context 'jsonSchema validator on collection' do
    let(:local_schema) { nil }

    before do
      client['users',
        {
          'validator' => { '$jsonSchema' => schema_map }
        }
      ].create
    end
  end

  shared_context 'schema map in client options' do
    let(:local_schema) { { "auto_encryption.users" => schema_map } }

    before do
      client['users'].create
    end
  end

  shared_context 'encrypted document in collection' do
    before do
      client['users'].insert_one(ssn: encrypted_ssn_binary)
    end
  end

  shared_context 'multiple encrypted documents in collection' do
    before do
      client['users'].insert_one(ssn: encrypted_ssn_binary)
      client['users'].insert_one(ssn: encrypted_ssn_binary)
    end
  end

  shared_context 'limited connection pool' do
    let(:max_pool_size) do
      1
    end
  end

  before(:each) do
    client['users'].drop
    key_vault_collection.drop
    key_vault_collection.insert_one(data_key)
  end

  shared_examples 'an encrypted command' do
    context 'with AWS KMS provider' do
      include_context 'with AWS kms_providers'

      context 'with validator' do
        include_context 'jsonSchema validator on collection'
        it_behaves_like 'it performs an encrypted command'
      end

      context 'with schema map' do
        include_context 'schema map in client options'
        it_behaves_like 'it performs an encrypted command'

        context 'with limited connection pool' do
          include_context 'limited connection pool'
          it_behaves_like 'it performs an encrypted command'
        end
      end
    end

    context 'with Azure KMS provider' do
      include_context 'with Azure kms_providers'

      context 'with validator' do
        include_context 'jsonSchema validator on collection'
        it_behaves_like 'it performs an encrypted command'
      end

      context 'with schema map' do
        include_context 'schema map in client options'
        it_behaves_like 'it performs an encrypted command'

        context 'with limited connection pool' do
          include_context 'limited connection pool'
          it_behaves_like 'it performs an encrypted command'
        end
      end
    end

    context 'with GCP KMS provider' do
      include_context 'with GCP kms_providers'

      context 'with validator' do
        include_context 'jsonSchema validator on collection'
        it_behaves_like 'it performs an encrypted command'
      end

      context 'with schema map' do
        include_context 'schema map in client options'
        it_behaves_like 'it performs an encrypted command'

        context 'with limited connection pool' do
          include_context 'limited connection pool'
          it_behaves_like 'it performs an encrypted command'
        end
      end
    end

    context 'with KMIP KMS provider' do
      include_context 'with KMIP kms_providers'

      context 'with validator' do
        include_context 'jsonSchema validator on collection'
        it_behaves_like 'it performs an encrypted command'
      end

      context 'with schema map' do
        include_context 'schema map in client options'
        it_behaves_like 'it performs an encrypted command'

        context 'with limited connection pool' do
          include_context 'limited connection pool'
          it_behaves_like 'it performs an encrypted command'
        end
      end
    end

    context 'with local KMS provider' do
      include_context 'with local kms_providers'

      context 'with validator' do
        include_context 'jsonSchema validator on collection'
        it_behaves_like 'it performs an encrypted command'
      end

      context 'with schema map' do
        include_context 'schema map in client options'
        it_behaves_like 'it performs an encrypted command'

        context 'with limited connection pool' do
          include_context 'limited connection pool'
          it_behaves_like 'it performs an encrypted command'
        end
      end
    end
  end

  describe '#aggregate' do
    shared_examples 'it performs an encrypted command' do
      include_context 'encrypted document in collection'

      let(:result) do
        encryption_client['users'].aggregate([
          { '$match' => { 'ssn' => ssn } }
        ]).first
      end

      it 'encrypts the command and decrypts the response' do
        result.should_not be_nil
        result['ssn'].should == ssn
      end

      context 'when bypass_auto_encryption=true' do
        include_context 'bypass auto encryption'

        it 'does not encrypt the command' do
          result.should be_nil
        end

        it 'does auto decrypt the response' do
          result = encryption_client['users'].aggregate([
            { '$match' => { 'ssn' => encrypted_ssn_binary } }
          ]).first

          result.should_not be_nil
          result['ssn'].should == ssn
        end
      end
    end

    it_behaves_like 'an encrypted command'
  end

  describe '#count' do
    shared_examples 'it performs an encrypted command' do
      include_context 'multiple encrypted documents in collection'

      let(:result) { encryption_client['users'].count(ssn: ssn) }

      it 'encrypts the command and finds the documents' do
        expect(result).to eq(2)
      end

      context 'with bypass_auto_encryption=true' do
        include_context 'bypass auto encryption'

        it 'does not encrypt the command' do
          expect(result).to eq(0)
        end
      end
    end

    it_behaves_like 'an encrypted command'
  end

  describe '#distinct' do
    shared_examples 'it performs an encrypted command' do
      include_context 'encrypted document in collection'

      let(:result) { encryption_client['users'].distinct(:ssn) }

      it 'decrypts the SSN field' do
        expect(result.length).to eq(1)
        expect(result).to include(ssn)
      end

      context 'with bypass_auto_encryption=true' do
        include_context 'bypass auto encryption'

        it 'still decrypts the SSN field' do
          expect(result.length).to eq(1)
          expect(result).to include(ssn)
        end
      end
    end

    it_behaves_like 'an encrypted command'
  end

  describe '#delete_one' do
    shared_examples 'it performs an encrypted command' do
      include_context 'encrypted document in collection'

      let(:result) { encryption_client['users'].delete_one(ssn: ssn) }

      it 'encrypts the SSN field' do
        expect(result.deleted_count).to eq(1)
      end

      context 'with bypass_auto_encryption=true' do
        include_context 'bypass auto encryption'

        it 'does not encrypt the SSN field' do
          expect(result.deleted_count).to eq(0)
        end
      end
    end

    it_behaves_like 'an encrypted command'
  end

  describe '#delete_many' do
    shared_examples 'it performs an encrypted command' do
      include_context 'multiple encrypted documents in collection'

      let(:result) { encryption_client['users'].delete_many(ssn: ssn) }

      it 'decrypts the SSN field' do
        expect(result.deleted_count).to eq(2)
      end

      context 'with bypass_auto_encryption=true' do
        include_context 'bypass auto encryption'

        it 'does not encrypt the SSN field' do
          expect(result.deleted_count).to eq(0)
        end
      end
    end

    it_behaves_like 'an encrypted command'
  end

  describe '#find' do
    shared_examples 'it performs an encrypted command' do
      include_context 'encrypted document in collection'

      let(:result) { encryption_client['users'].find(ssn: ssn).first }

      it 'encrypts the command and decrypts the response' do
        result.should_not be_nil
        expect(result['ssn']).to eq(ssn)
      end

      context 'when bypass_auto_encryption=true' do
        include_context 'bypass auto encryption'

        it 'does not encrypt the command' do
          expect(result).to be_nil
        end
      end
    end

    it_behaves_like 'an encrypted command'
  end

  describe '#find_one_and_delete' do
    shared_examples 'it performs an encrypted command' do
      include_context 'encrypted document in collection'

      let(:result) { encryption_client['users'].find_one_and_delete(ssn: ssn) }

      it 'encrypts the command and decrypts the response' do
        expect(result['ssn']).to eq(ssn)
      end

      context 'when bypass_auto_encryption=true' do
        include_context 'bypass auto encryption'

        it 'does not encrypt the command' do
          expect(result).to be_nil
        end

        it 'still decrypts the command' do
          result = encryption_client['users'].find_one_and_delete(ssn: encrypted_ssn_binary)
          expect(result['ssn']).to eq(ssn)
        end
      end
    end

    it_behaves_like 'an encrypted command'
  end

  describe '#find_one_and_replace' do
    shared_examples 'it performs an encrypted command' do
      let(:name) { 'Alan Turing' }

      context 'with :return_document => :before' do
        include_context 'encrypted document in collection'

        let(:result) do
          encryption_client['users'].find_one_and_replace(
            { ssn: ssn },
            { name: name },
            return_document: :before
          )
        end

        it 'encrypts the command and decrypts the response, returning original document' do
          expect(result['ssn']).to eq(ssn)

          documents = client['users'].find
          expect(documents.count).to eq(1)
          expect(documents.first['ssn']).to be_nil
        end
      end

      context 'with :return_document => :after' do
        before do
          client['users'].insert_one(name: name)
        end

        let(:result) do
          encryption_client['users'].find_one_and_replace(
            { name: name },
            { ssn: ssn },
            return_document: :after
          )
        end

        it 'encrypts the command and decrypts the response, returning new document' do
          expect(result['ssn']).to eq(ssn)

          documents = client['users'].find
          expect(documents.count).to eq(1)
          expect(documents.first['ssn']).to eq(encrypted_ssn_binary)
        end
      end

      context 'when bypass_auto_encryption=true' do
        include_context 'bypass auto encryption'
        include_context 'encrypted document in collection'

        let(:result) do
          encryption_client['users'].find_one_and_replace(
            { ssn: encrypted_ssn_binary },
            { name: name },
            :return_document => :before
          )
        end

        it 'does not encrypt the command but still decrypts the response, returning original document' do
          expect(result['ssn']).to eq(ssn)

          documents = client['users'].find
          expect(documents.count).to eq(1)
          expect(documents.first['ssn']).to be_nil
        end
      end
    end

    it_behaves_like 'an encrypted command'
  end

  describe '#find_one_and_update' do
    shared_examples 'it performs an encrypted command' do
      include_context 'encrypted document in collection'

      let(:name) { 'Alan Turing' }

      let(:result) do
        encryption_client['users'].find_one_and_update(
          { ssn: ssn },
          { name: name }
        )
      end

      it 'encrypts the command and decrypts the response' do
        expect(result['ssn']).to eq(ssn)

        documents = client['users'].find
        expect(documents.count).to eq(1)
        expect(documents.first['ssn']).to be_nil
      end

      context 'with bypass_auto_encryption=true' do
        include_context 'bypass auto encryption'

        it 'does not encrypt the command' do
          expect(result).to be_nil
        end

        it 'still decrypts the response' do
          # Query using the encrypted ssn value so the find will succeed
          result = encryption_client['users'].find_one_and_update(
            { ssn: encrypted_ssn_binary },
            { name: name }
          )

          expect(result['ssn']).to eq(ssn)
        end
      end
    end

    it_behaves_like 'an encrypted command'
  end

  describe '#insert_one' do
    let(:query) { { ssn: ssn } }
    let(:result) { encryption_client['users'].insert_one(query) }

    shared_examples 'it performs an encrypted command' do
      it 'encrypts the ssn field' do
        expect(result).to be_ok
        expect(result.inserted_ids.length).to eq(1)

        id = result.inserted_ids.first

        document = client['users'].find(_id: id).first
        document.should_not be_nil
        expect(document['ssn']).to eq(encrypted_ssn_binary)
      end
    end

    shared_examples 'it obeys bypass_auto_encryption option' do
      include_context 'bypass auto encryption'

      it 'does not encrypt the command' do
        result = encryption_client['users'].insert_one(ssn: ssn)
        expect(result).to be_ok
        expect(result.inserted_ids.length).to eq(1)

        id = result.inserted_ids.first

        document = client['users'].find(_id: id).first
        expect(document['ssn']).to eq(ssn)
      end
    end

    it_behaves_like 'an encrypted command'

    context 'with jsonSchema in schema_map option' do
      include_context 'schema map in client options'

      context 'with AWS KMS provider' do
        include_context 'with AWS kms_providers'
        it_behaves_like 'it obeys bypass_auto_encryption option'
      end

      context 'with Azure KMS provider' do
        include_context 'with Azure kms_providers'
        it_behaves_like 'it obeys bypass_auto_encryption option'
      end

      context 'with GCP KMS provider' do
        include_context 'with GCP kms_providers'
        it_behaves_like 'it obeys bypass_auto_encryption option'
      end

      context 'with KMIP KMS provider' do
        include_context 'with KMIP kms_providers'
        it_behaves_like 'it obeys bypass_auto_encryption option'
      end


      context 'with local KMS provider and ' do
        include_context 'with local kms_providers'
        it_behaves_like 'it obeys bypass_auto_encryption option'
      end
    end

    context 'with schema_map client option pointing to wrong collection' do
      let(:local_schema) { { 'wrong_db.wrong_coll' => schema_map } }

      include_context 'with local kms_providers'

      it 'does not raise an exception but doesn\'t encrypt either' do
        expect do
          result
        end.not_to raise_error

        expect(result).to be_ok
        id = result.inserted_ids.first

        document = client['users'].find(_id: id).first
        document.should_not be_nil
        # Document was not encrypted
        expect(document['ssn']).to eq(ssn)
      end
    end

    context 'encrypting using key alt name' do
      include_context 'schema map in client options'

      let(:query) { { ssn: ssn, altname: key_alt_name } }

      context 'with AWS KMS provider' do
        include_context 'with AWS kms_providers and key alt names'
        it 'encrypts the ssn field' do
          expect(result).to be_ok
          expect(result.inserted_ids.length).to eq(1)

          id = result.inserted_ids.first

          document = client['users'].find(_id: id).first
          document.should_not be_nil
          # Auto-encryption with key alt names only works with random encryption,
          # so it will not generate the same result on every test run.
          expect(document['ssn']).to be_ciphertext
        end
      end

      context 'with Azure KMS provider' do
        include_context 'with Azure kms_providers and key alt names'
        it 'encrypts the ssn field' do
          expect(result).to be_ok
          expect(result.inserted_ids.length).to eq(1)

          id = result.inserted_ids.first

          document = client['users'].find(_id: id).first
          document.should_not be_nil
          # Auto-encryption with key alt names only works with random encryption,
          # so it will not generate the same result on every test run.
          expect(document['ssn']).to be_ciphertext
        end

        context 'with GCP KMS provider' do
          include_context 'with GCP kms_providers and key alt names'
          it 'encrypts the ssn field' do
            expect(result).to be_ok
            expect(result.inserted_ids.length).to eq(1)

            id = result.inserted_ids.first

            document = client['users'].find(_id: id).first
            document.should_not be_nil
            # Auto-encryption with key alt names only works with random encryption,
            # so it will not generate the same result on every test run.
            expect(document['ssn']).to be_ciphertext
          end
        end

        context 'with KMIP KMS provider' do
          include_context 'with KMIP kms_providers and key alt names'
          it 'encrypts the ssn field' do
            expect(result).to be_ok
            expect(result.inserted_ids.length).to eq(1)

            id = result.inserted_ids.first

            document = client['users'].find(_id: id).first
            document.should_not be_nil
            # Auto-encryption with key alt names only works with random encryption,
            # so it will not generate the same result on every test run.
            expect(document['ssn']).to be_ciphertext
          end
        end
      end

      context 'with local KMS provider' do
        include_context 'with local kms_providers and key alt names'
        it 'encrypts the ssn field' do
          expect(result).to be_ok
          expect(result.inserted_ids.length).to eq(1)

          id = result.inserted_ids.first

          document = client['users'].find(_id: id).first
          document.should_not be_nil
          # Auto-encryption with key alt names only works with random encryption,
          # so it will not generate the same result on every test run.
          expect(document['ssn']).to be_a_kind_of(BSON::Binary)
        end
      end
    end
  end

  describe '#replace_one' do
    shared_examples 'it performs an encrypted command' do
      include_context 'encrypted document in collection'

      let(:replacement_ssn) { '098-765-4321' }

      let(:result) do
        encryption_client['users'].replace_one(
          { ssn: ssn },
          { ssn: replacement_ssn }
        )
      end

      it 'encrypts the ssn field' do
        expect(result.modified_count).to eq(1)

        find_result = encryption_client['users'].find(ssn: '098-765-4321')
        expect(find_result.count).to eq(1)
      end

      context 'with bypass_auto_encryption=true' do
        include_context 'bypass auto encryption'

        it 'does not encrypt the command' do
          expect(result.modified_count).to eq(0)
        end
      end
    end

    it_behaves_like 'an encrypted command'
  end

  describe '#update_one' do
    shared_examples 'it performs an encrypted command' do
      include_context 'encrypted document in collection'

      let(:result) do
        encryption_client['users'].replace_one({ ssn: ssn }, { ssn: '098-765-4321' })
      end

      it 'encrypts the ssn field' do
        expect(result.n).to eq(1)

        find_result = encryption_client['users'].find(ssn: '098-765-4321')
        expect(find_result.count).to eq(1)
      end

      context 'with bypass_auto_encryption=true' do
        include_context 'bypass auto encryption'

        it 'does not encrypt the command' do
          expect(result.n).to eq(0)
        end
      end
    end

    it_behaves_like 'an encrypted command'
  end

  describe '#update_many' do
    shared_examples 'it performs an encrypted command' do
      before do
        client['users'].insert_one(ssn: encrypted_ssn_binary, age: 25)
        client['users'].insert_one(ssn: encrypted_ssn_binary, age: 43)
      end

      let(:result) do
        encryption_client['users'].update_many({ ssn: ssn }, { "$inc" => { :age =>  1 } })
      end

      it 'encrypts the ssn field' do
        expect(result.n).to eq(2)

        updated_documents = encryption_client['users'].find(ssn: ssn)
        ages = updated_documents.map { |doc| doc['age'] }
        expect(ages).to include(26)
        expect(ages).to include(44)
      end

      context 'with bypass_auto_encryption=true' do
        include_context 'bypass auto encryption'

        it 'does not encrypt the command' do
          expect(result.n).to eq(0)
        end
      end
    end

    it_behaves_like 'an encrypted command'
  end
end
