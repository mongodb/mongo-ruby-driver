require 'spec_helper'

describe 'CRUD operations' do
  let(:collection) { authorized_client['crud_integration'] }

  before do
    collection.delete_many
  end

  describe 'find' do
    context 'when allow_disk_use is true' do
      # Other cases are adequately covered by spec tests.
      context 'on server version < 3.2' do
        max_server_fcv '3.0'

        it 'raises an exception' do
          expect do
            collection.find({}, { allow_disk_use: true }).first
          end.to raise_error(Mongo::Error::UnsupportedOption, /The MongoDB server handling this request does not support the allow_disk_use option on this command./)
        end
      end
    end

    context 'when allow_disk_use is false' do
      # Other cases are adequately covered by spec tests.
      context 'on server version < 3.2' do
        max_server_fcv '3.0'

        it 'raises an exception' do
          expect do
            collection.find({}, { allow_disk_use: false }).first
          end.to raise_error(Mongo::Error::UnsupportedOption, /The MongoDB server handling this request does not support the allow_disk_use option on this command./)
        end
      end
    end
  end

  describe 'insert' do
    context 'inserting a BSON::Int64' do
      before do
        collection.insert_one(int64: BSON::Int64.new(42))
      end

      it 'is stored as the correct type' do
        # 18 is the number that represents the Int64 type for the $type
        # operator; string aliases in the $type operator are only supported on
        # server versions 3.2 and newer.
        result = collection.find(int64: { '$type' => 18 }).first
        expect(result).not_to be_nil
        expect(result['int64']).to eq(42)
      end
    end

    context 'inserting a BSON::Int32' do
      before do
        collection.insert_one(int32: BSON::Int32.new(42))
      end

      it 'is stored as the correct type' do
        # 16 is the number that represents the Int32 type for the $type
        # operator; string aliases in the $type operator are only supported on
        # server versions 3.2 and newer.
        result = collection.find(int32: { '$type' => 16 }).first
        expect(result).not_to be_nil
        expect(result['int32']).to eq(42)
      end
    end

    context 'with automatic encryption' do
      require_libmongocrypt
      require_enterprise
      min_server_fcv '4.2'

      include_context 'define shared FLE helpers'
      include_context 'with local kms_providers'

      let(:encrypted_collection) do
        new_local_client(
          SpecConfig.instance.addresses,
          SpecConfig.instance.test_options.merge(
            auto_encryption_options: {
              kms_providers: kms_providers,
              key_vault_namespace: key_vault_namespace,
              schema_map: { 'auto_encryption.users' => schema_map },
              # Spawn mongocryptd on non-default port for sharded cluster tests
              extra_options: extra_options,
            },
            database: 'auto_encryption'
          )
        )['users']
      end

      let(:collection) { authorized_client.use('auto_encryption')['users'] }

      context 'inserting a BSON::Int64' do
        before do
          encrypted_collection.insert_one(ssn: '123-456-7890', int64: BSON::Int64.new(42))
        end

        it 'is stored as the correct type' do
          # 18 is the number that represents the Int64 type for the $type
          # operator; string aliases in the $type operator are only supported on
          # server versions 3.2 and newer.
          result = collection.find(int64: { '$type' => 18 }).first
          expect(result).not_to be_nil
          expect(result['int64']).to eq(42)
        end
      end

      context 'inserting a BSON::Int32' do
        before do
          encrypted_collection.insert_one(ssn: '123-456-7890', int32: BSON::Int32.new(42))
        end

        it 'is stored as the correct type' do
          # 16 is the number that represents the Int32 type for the $type
          # operator; string aliases in the $type operator are only supported on
          # server versions 3.2 and newer.
          result = collection.find(int32: { '$type' => 16 }).first
          expect(result).not_to be_nil
          expect(result['int32']).to eq(42)
        end
      end
    end
  end

  describe 'upsert' do
    context 'with default write concern' do
      it 'upserts' do
        collection.count_documents.should == 0

        res = collection.find(_id: 'foo').update_one({'$set' => {foo: 'bar'}}, upsert: true)

        res.documents.first['upserted'].length.should == 1

        collection.count_documents.should == 1
      end
    end

    context 'unacknowledged write' do
      let(:unack_collection) do
        collection.with(write_concern: {w: 0})
      end

      before do
        unack_collection.write_concern.acknowledged?.should be false
      end

      it 'upserts' do
        unack_collection.count_documents.should == 0

        res = unack_collection.find(_id: 'foo').update_one({'$set' => {foo: 'bar'}}, upsert: true)

        # since write concern is unacknowledged, wait for the data to be
        # persisted (hopefully)
        sleep 0.25

        unack_collection.count_documents.should == 1
      end
    end
  end
end
