# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'CRUD operations' do
  let(:client) { authorized_client }
  let(:collection) { client['crud_integration'] }

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

    context 'when using the legacy $query syntax' do
      before do
        collection.insert_one(_id: 1, test: 1)
        collection.insert_one(_id: 2, test: 2)
        collection.insert_one(_id: 3, test: 3)
      end

      context 'filter only' do
        it 'passes the filter' do
          collection.find(:'$query' => {test: 1}).first.should == {'_id' => 1, 'test' => 1}
        end
      end

      context 'empty filter with order' do
        it 'passes the filter' do
          collection.find(:'$query' => {}, :'$orderby' => {test: 1}).first.should == {'_id' => 1, 'test' => 1}
          collection.find(:'$query' => {}, :'$orderby' => {test: -1}).first.should == {'_id' => 3, 'test' => 3}
        end
      end

      context 'filter with order' do
        it 'passes both filter and order' do
          collection.find(:'$query' => {test: {'$gt' => 1}}, '$orderby' => {test: 1}).first.should == {'_id' => 2, 'test' => 2}
          collection.find(:'$query' => {test: {'$gt' => 1}}, '$orderby' => {test: -1}).first.should == {'_id' => 3, 'test' => 3}
        end
      end
    end

    context 'with read concern' do
      # Read concern requires 3.2+ server.
      min_server_fcv '3.2'

      context 'with read concern specified on operation level' do

        it 'passes the read concern' do
          event = Utils.get_command_event(client, 'find') do |client|
            client['foo'].find({}, read_concern: {level: :local}).to_a
          end
          event.command.fetch('readConcern').should == {'level' => 'local'}
        end
      end

      context 'with read concern specified on collection level' do

        it 'passes the read concern' do
          event = Utils.get_command_event(client, 'find') do |client|
            client['foo', read_concern: {level: :local}].find.to_a
          end
          event.command.fetch('readConcern').should == {'level' => 'local'}
        end
      end

      context 'with read concern specified on client level' do

        let(:client) { authorized_client.with(read_concern: {level: :local}) }

        it 'passes the read concern' do
          event = Utils.get_command_event(client, 'find') do |client|
            client['foo'].find.to_a
          end
          event.command.fetch('readConcern').should == {'level' => 'local'}
        end
      end
    end

    context 'with oplog_replay option' do
      let(:collection_name) { 'crud_integration_oplog_replay' }

      let(:oplog_query) do
        {ts: {'$gt' => 1}}
      end

      context 'passed to operation' do
        it 'passes the option' do
          event = Utils.get_command_event(client, 'find') do |client|
            client[collection_name].find(oplog_query, oplog_replay: true).to_a
          end
          event.command.fetch('oplogReplay').should be true
        end

        it 'warns' do
          client.should receive(:log_warn).with('The :oplog_replay option is deprecated and ignored by MongoDB 4.4 and later')
          client[collection_name].find(oplog_query, oplog_replay: true).to_a
        end
      end

      context 'set on collection' do
        it 'passes the option' do
          event = Utils.get_command_event(client, 'find') do |client|
            client[collection_name, oplog_replay: true].find(oplog_query).to_a
          end
          event.command.fetch('oplogReplay').should be true
        end

        it 'warns' do
          client.should receive(:log_warn).with('The :oplog_replay option is deprecated and ignored by MongoDB 4.4 and later')
          client[collection_name, oplog_replay: true].find(oplog_query).to_a
        end
      end
    end
  end

  describe 'explain' do
    context 'with explicit session' do
      min_server_fcv '3.6'

      it 'passes the session' do
        client.start_session do |session|
          event = Utils.get_command_event(client, 'explain') do |client|
            client['foo'].find({}, session: session).explain.should be_explain_output
          end
          event.command.fetch('lsid').should == session.session_id
        end
      end
    end

    context 'with read preference specified on operation level' do
      require_topology :sharded

      # RUBY-2706
      min_server_fcv '3.6'

      it 'passes the read preference' do
        event = Utils.get_command_event(client, 'explain') do |client|
          client['foo'].find({}, read: {mode: :secondary_preferred}).explain.should be_explain_output
        end
        event.command.fetch('$readPreference').should == {'mode' => 'secondaryPreferred'}
      end
    end

    context 'with read preference specified on collection level' do
      require_topology :sharded

      # RUBY-2706
      min_server_fcv '3.6'

      it 'passes the read preference' do
        event = Utils.get_command_event(client, 'explain') do |client|
          client['foo', read: {mode: :secondary_preferred}].find.explain.should be_explain_output
        end
        event.command.fetch('$readPreference').should == {'mode' => 'secondaryPreferred'}
      end
    end

    context 'with read preference specified on client level' do
      require_topology :sharded

      # RUBY-2706
      min_server_fcv '3.6'

      let(:client) { authorized_client.with(read: {mode: :secondary_preferred}) }

      it 'passes the read preference' do
        event = Utils.get_command_event(client, 'explain') do |client|
          client['foo'].find.explain.should be_explain_output
        end
        event.command.fetch('$readPreference').should == {'mode' => 'secondaryPreferred'}
      end
    end

    context 'with read concern' do
      # Read concern requires 3.2+ server.
      min_server_fcv '3.2'

      context 'with read concern specifed on operation level' do

        # Read concern is not allowed in explain command, driver drops it.
        it 'drops the read concern' do
          event = Utils.get_command_event(client, 'explain') do |client|
            client['foo'].find({}, read_concern: {level: :local}).explain.should have_key('queryPlanner')
          end
          event.command.should_not have_key('readConcern')
        end
      end

      context 'with read concern specifed on collection level' do

        # Read concern is not allowed in explain command, driver drops it.
        it 'drops the read concern' do
          event = Utils.get_command_event(client, 'explain') do |client|
            client['foo', read_concern: {level: :local}].find.explain.should have_key('queryPlanner')
          end
          event.command.should_not have_key('readConcern')
        end
      end

      context 'with read concern specifed on client level' do

        let(:client) { authorized_client.with(read_concern: {level: :local}) }

        # Read concern is not allowed in explain command, driver drops it.
        it 'drops the read concern' do
          event = Utils.get_command_event(client, 'explain') do |client|
            client['foo'].find.explain.should have_key('queryPlanner')
          end
          event.command.should_not have_key('readConcern')
        end
      end
    end
  end

  describe 'insert' do
    context 'user documents' do
      let(:doc) do
        IceNine.deep_freeze(test: 42)
      end

      it 'does not mutate user documents' do
        lambda do
          collection.insert_one(doc)
        end.should_not raise_error
      end
    end

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
