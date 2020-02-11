require 'spec_helper'
require 'bson'
require 'json'

describe 'Auto Encryption' do
  require_libmongocrypt
  require_enterprise

  include_context 'define shared FLE helpers'

  let(:encryption_client) do
    new_local_client(
      SpecConfig.instance.addresses,
      SpecConfig.instance.test_options.merge(
        auto_encryption_options: {
          kms_providers: kms_providers,
          key_vault_namespace: key_vault_namespace,
          schema_map: local_schema,
          bypass_auto_encryption: bypass_auto_encryption
        },
        database: 'auto-encryption'
      ),
    )
  end

  let(:client) do
    authorized_client.use('auto-encryption')
  end

  let(:bypass_auto_encryption) { false }

  let(:encrypted_ssn_binary) do
    BSON::Binary.new(Base64.decode64(encrypted_ssn), :ciphertext)
  end

  shared_context 'bypass auto encryption' do
    let(:bypass_auto_encryption) { true }
  end

  shared_context 'jsonSchema validator on collection' do
    let(:local_schema) { nil }

    before do
      client[:users,
        {
          'validator' => { '$jsonSchema' => schema_map }
        }
      ].create
    end
  end

  shared_context 'schema map in client options' do
    let(:local_schema) { { "auto-encryption.users" => schema_map } }

    before do
      client[:users].create
    end
  end

  before(:each) do
    client[:users].drop
    client.use(:admin)[:datakeys].drop
    client.use(:admin)[:datakeys].insert_one(data_key)
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
      end
    end
  end

  describe '#aggregate' do
    shared_examples 'it performs an encrypted command' do
      before do
        client[:users].insert_one(ssn: encrypted_ssn_binary)
      end

      it 'encrypts the command and decrypts the response' do
        document = encryption_client[:users].aggregate([
          { '$match' => { 'ssn' => ssn } }
        ]).first

        document.should_not be_nil
        document['ssn'].should == ssn
      end

      context 'when bypass_auto_encryption=true' do
        include_context 'bypass auto encryption'

        it 'does not encrypt the command' do
          document = encryption_client[:users].aggregate([
            { '$match' => { 'ssn' => ssn } }
          ]).first

          document.should be_nil
        end

        it 'does auto decrypt the response' do
          document = encryption_client[:users].aggregate([
            { '$match' => { 'ssn' => encrypted_ssn_binary } }
          ]).first

          document.should_not be_nil
          document['ssn'].should == ssn
        end
      end
    end

    it_behaves_like 'an encrypted command'
  end

  describe '#count' do
    shared_examples 'it performs an encrypted command' do
      before do
        client[:users].insert_one(ssn: encrypted_ssn_binary)
        client[:users].insert_one(ssn: encrypted_ssn_binary)
      end

      it 'encrypts the command and finds the documents' do
        count = encryption_client[:users].count(ssn: ssn)
        count.should == 2
      end

      context 'with bypass_auto_encryption=true' do
        include_context 'bypass auto encryption'

        it 'does not encrypt the command' do
          count = encryption_client[:users].count(ssn: ssn)
          count.should == 0
        end
      end
    end

    it_behaves_like 'an encrypted command'
  end

  describe '#distinct' do
    shared_examples 'it performs an encrypted command' do
      before do
        client[:users].insert_one(ssn: encrypted_ssn_binary)
      end

      it 'decrypts the SSN field' do
        values = encryption_client[:users].distinct(:ssn)
        values.length.should == 1
        values.should include(ssn)
      end

      context 'with bypass_auto_encryption=true' do
        it 'still decrypts the SSN field' do
          values = encryption_client[:users].distinct(:ssn)
          values.length.should == 1
          values.should include(ssn)
        end
      end
    end

    it_behaves_like 'an encrypted command'
  end

  describe '#delete_one' do
    shared_examples 'it performs an encrypted command' do
      before do
        client[:users].insert_one(ssn: encrypted_ssn_binary)
      end

      it 'encrypts the SSN field' do
        result = encryption_client[:users].delete_one(ssn: ssn)
        expect(result.deleted_count).to eq(1)
      end
    end

    it_behaves_like 'an encrypted command'
  end

  describe '#delete_many' do
    shared_examples 'it performs an encrypted command' do
      before do
        client[:users].insert_many([
          { ssn: encrypted_ssn_binary },
          { ssn: encrypted_ssn_binary }
        ])
      end

      it 'decrypts the SSN field' do
        result = encryption_client[:users].delete_many(ssn: ssn)
        expect(result.deleted_count).to eq(2)
      end
    end

    it_behaves_like 'an encrypted command'
  end

  describe '#find' do
    shared_examples 'it performs an encrypted command' do
      before do
        client[:users].insert_one(ssn: encrypted_ssn_binary)
      end

      it 'encrypts the command and decrypts the response' do
        document = encryption_client[:users].find(ssn: ssn).first
        document.should_not be_nil
        expect(document['ssn']).to eq(ssn)
      end

      context 'when bypass_auto_encryption=true' do
        include_context 'bypass auto encryption'

        it 'does not encrypt the command' do
          document = encryption_client[:users].find(ssn: ssn).first
          expect(document).to be_nil
        end
      end
    end

    it_behaves_like 'an encrypted command'
  end

  describe '#insert_one' do
    let(:client_collection) { client[:users] }

    shared_examples 'it performs an encrypted command' do
      it 'encrypts the ssn field' do
        result = encryption_client[:users].insert_one(ssn: ssn)
        expect(result).to be_ok
        expect(result.inserted_ids.length).to eq(1)

        id = result.inserted_ids.first

        document = client_collection.find(_id: id).first
        document.should_not be_nil
        expect(document['ssn']).to eq(encrypted_ssn_binary)
      end
    end

    # TODO: fix this
    shared_examples 'it obeys bypass_auto_encryption option' do
      include_context 'bypass auto encryption'

      it 'does not encrypt the command' do
        result = encryption_client[:users].insert_one(ssn: ssn)
        expect(result).to be_ok
        expect(result.inserted_ids.length).to eq(1)

        id = result.inserted_ids.first

        document = client[:users].find(_id: id).first
        expect(document['ssn']).to eq(ssn)
      end
    end

    it_behaves_like 'an encrypted command'
  end

  describe '#update_one' do
    shared_examples 'it performs an encrypted command' do
      before do
        client[:users].insert_one(ssn: encrypted_ssn_binary)
      end

      it 'encrypts the ssn field' do
        result = encryption_client[:users].find(ssn: ssn).update_one(ssn: '098-765-4321')
        expect(result.n).to eq(1)
      end
    end

    it_behaves_like 'an encrypted command'
  end
end
