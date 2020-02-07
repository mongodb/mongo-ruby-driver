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

  describe '#aggregate' do
    shared_examples 'it performs encrypted aggregation' do
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

    context 'with AWS KMS provider' do
      include_context 'with AWS kms_providers'

      context 'with validator' do
        include_context 'jsonSchema validator on collection'
        it_behaves_like 'it performs encrypted aggregation'
      end

      context 'with schema map' do
        include_context 'schema map in client options'
        it_behaves_like 'it performs encrypted aggregation'
      end
    end

    context 'with local KMS provider' do
      include_context 'with local kms_providers'

      context 'with validator' do
        include_context 'jsonSchema validator on collection'
        it_behaves_like 'it performs encrypted aggregation'
      end

      context 'with schema map' do
        include_context 'schema map in client options'
        it_behaves_like 'it performs encrypted aggregation'
      end
    end
  end

  describe '#count' do
    shared_examples 'it performs encrypted count' do
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

    context 'with AWS KMS provider' do
      include_context 'with AWS kms_providers'

      context 'with validator' do
        include_context 'jsonSchema validator on collection'
        it_behaves_like 'it performs encrypted count'
      end

      context 'with schema map' do
        include_context 'schema map in client options'
        it_behaves_like 'it performs encrypted count'
      end
    end

    context 'with local KMS provider' do
      include_context 'with local kms_providers'

      context 'with validator' do
        include_context 'jsonSchema validator on collection'
        it_behaves_like 'it performs encrypted count'
      end

      context 'with schema map' do
        include_context 'schema map in client options'
        it_behaves_like 'it performs encrypted count'
      end
    end
  end

  describe '#distinct' do
    shared_examples 'it performs encrypted distinct' do
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

    context 'with local KMS provider' do
      include_context 'with local kms_providers'

      context 'with validator' do
        include_context 'jsonSchema validator on collection'
        it_behaves_like 'it performs encrypted distinct'
      end

      context 'with schema map' do
        include_context 'schema map in client options'
        it_behaves_like 'it performs encrypted distinct'
      end
    end

    context 'with AWS KMS provider' do
      include_context 'with AWS kms_providers'

      context 'with validator' do
        include_context 'jsonSchema validator on collection'
        it_behaves_like 'it performs encrypted distinct'
      end

      context 'with schema map' do
        include_context 'schema map in client options'
        it_behaves_like 'it performs encrypted distinct'
      end
    end
  end

  describe '#insert_one' do
    let(:client_collection) { client[:users] }

    shared_examples 'it performs encrypted inserts' do
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

    context 'with AWS KMS provider' do
      include_context 'with AWS kms_providers'

      context 'with validator' do
        include_context 'jsonSchema validator on collection'
        it_behaves_like 'it performs encrypted inserts'
      end

      context 'with schema map' do
        include_context 'schema map in client options'
        it_behaves_like 'it performs encrypted inserts'
        it_behaves_like 'it obeys bypass_auto_encryption option'
      end
    end

    context 'with local KMS provider' do
      include_context 'with local kms_providers'

      context 'with validator' do
        include_context 'jsonSchema validator on collection'
        it_behaves_like 'it performs encrypted inserts'
      end

      context 'with schema map' do
        include_context 'schema map in client options'
        it_behaves_like 'it performs encrypted inserts'
        it_behaves_like 'it obeys bypass_auto_encryption option'
      end
    end
  end

  describe '#find' do
    shared_examples 'it performs encrypted finds' do
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

    context 'with AWS KMS provider' do
      include_context 'with AWS kms_providers'

      context 'with validator' do
        include_context 'jsonSchema validator on collection'
        it_behaves_like 'it performs encrypted finds'
     end

      context 'with schema map' do
        include_context 'schema map in client options'
        it_behaves_like 'it performs encrypted finds'
     end
    end

    context 'with local KMS provider' do
      include_context 'with local kms_providers'

      context 'with validator' do
        include_context 'jsonSchema validator on collection'
        it_behaves_like 'it performs encrypted finds'
     end

      context 'with schema map' do
        include_context 'schema map in client options'
        it_behaves_like 'it performs encrypted finds'
     end
    end
  end
end
