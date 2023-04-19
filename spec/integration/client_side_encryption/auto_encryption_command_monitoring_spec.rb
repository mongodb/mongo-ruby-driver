# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'Auto Encryption' do
  require_libmongocrypt
  require_enterprise
  min_server_fcv '4.2'

  # Diagnostics of leaked background threads only, these tests do not
  # actually require a clean slate. https://jira.mongodb.org/browse/RUBY-2138
  clean_slate

  include_context 'define shared FLE helpers'
  include_context 'with local kms_providers'

  let(:subscriber) { Mrss::EventSubscriber.new }
  let(:db_name) { 'auto_encryption' }

  let(:encryption_client) do
    new_local_client(
      SpecConfig.instance.addresses,
      SpecConfig.instance.test_options.merge(
        auto_encryption_options: {
          kms_providers: kms_providers,
          key_vault_namespace: key_vault_namespace,
          schema_map: { "auto_encryption.users" => schema_map },
          # Spawn mongocryptd on non-default port for sharded cluster tests
          extra_options: extra_options,
        },
        database: db_name
      ),
    ).tap do |client|
      client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
    end
  end

  before(:each) do
    key_vault_collection.drop
    key_vault_collection.insert_one(data_key)

    encryption_client['users'].drop
  end

  let(:started_event) do
    subscriber.single_command_started_event(command_name, database_name: db_name)
  end

  let(:succeeded_event) do
    subscriber.single_command_succeeded_event(command_name, database_name: db_name)
  end

  let(:key_vault_list_collections_event) do
    subscriber.started_events.find do |event|
      event.command_name == 'listCollections' && event.database_name == key_vault_db
    end
  end

  shared_examples 'it has a non-encrypted key_vault_client' do
    it 'does not register a listCollections event on the key vault client' do
      expect(key_vault_list_collections_event).to be_nil
    end
  end

  context 'when performing operations that need a document in the database' do
    before do
      result = encryption_client['users'].insert_one(ssn: ssn, age: 23)
    end

    describe '#aggregate' do
      let(:command_name) { 'aggregate' }

      before do
        encryption_client['users'].aggregate([{ '$match' => { 'ssn' => ssn } }]).first
      end

      it 'has encrypted data in command monitoring' do
        # Command started event occurs after ssn is encrypted
        expect(
          started_event.command["pipeline"].first["$match"]["ssn"]["$eq"]
        ).to be_ciphertext

        # Command succeeded event occurs before ssn is decrypted
        expect(succeeded_event.reply["cursor"]["firstBatch"].first["ssn"]).to be_ciphertext
      end

      it_behaves_like 'it has a non-encrypted key_vault_client'
    end

    describe '#count' do
      let(:command_name) { 'count' }

      before do
        encryption_client['users'].count(ssn: ssn)
      end

      it 'has encrypted data in command monitoring' do
        # Command started event occurs after ssn is encrypted
        # Command succeeded event does not contain any data to be decrypted
        expect(started_event.command["query"]["ssn"]["$eq"]).to be_ciphertext
      end

      it_behaves_like 'it has a non-encrypted key_vault_client'
    end

    describe '#distinct' do
      let(:command_name) { 'distinct' }

      before do
        encryption_client['users'].distinct(:ssn)
      end

      it 'has encrypted data in command monitoring' do
        # Command started event does not contain any data to be encrypted
        # Command succeeded event occurs before ssn is decrypted
        expect(succeeded_event.reply["values"].first).to be_ciphertext
      end

      it_behaves_like 'it has a non-encrypted key_vault_client'
    end

    describe '#delete_one' do
      let(:command_name) { 'delete' }

      before do
        encryption_client['users'].delete_one(ssn: ssn)
      end

      it 'has encrypted data in command monitoring' do
        # Command started event occurs after ssn is encrypted
        # Command succeeded event does not contain any data to be decrypted
        expect(started_event.command["deletes"].first["q"]["ssn"]["$eq"]).to be_ciphertext
      end

      it_behaves_like 'it has a non-encrypted key_vault_client'
    end

    describe '#delete_many' do
      let(:command_name) { 'delete' }

      before do
        encryption_client['users'].delete_many(ssn: ssn)
      end

      it 'has encrypted data in command monitoring' do
        # Command started event occurs after ssn is encrypted
        # Command succeeded event does not contain any data to be decrypted
        expect(started_event.command["deletes"].first["q"]["ssn"]["$eq"]).to be_ciphertext
      end

      it_behaves_like 'it has a non-encrypted key_vault_client'
    end

    describe '#find' do
      let(:command_name) { 'find' }

      before do
        encryption_client['users'].find(ssn: ssn).first
      end

      it 'has encrypted data in command monitoring' do
        # Command started event occurs after ssn is encrypted
        expect(started_event.command["filter"]["ssn"]["$eq"]).to be_ciphertext

        # Command succeeded event occurs before ssn is decrypted
        expect(succeeded_event.reply["cursor"]["firstBatch"].first["ssn"]).to be_ciphertext
      end

      it_behaves_like 'it has a non-encrypted key_vault_client'
    end

    describe '#find_one_and_delete' do
      let(:command_name) { 'findAndModify' }

      before do
        encryption_client['users'].find_one_and_delete(ssn: ssn)
      end

      it 'has encrypted data in command monitoring' do
        # Command started event occurs after ssn is encrypted
        expect(started_event.command["query"]["ssn"]["$eq"]).to be_ciphertext

        # Command succeeded event occurs before ssn is decrypted
        expect(succeeded_event.reply["value"]["ssn"]).to be_ciphertext
      end

      it_behaves_like 'it has a non-encrypted key_vault_client'
    end

    describe '#find_one_and_replace' do
      let(:command_name) { 'findAndModify' }

      before do
        encryption_client['users'].find_one_and_replace(
          { ssn: ssn },
          { ssn: '555-555-5555' }
        )
      end

      it 'has encrypted data in command monitoring' do
        # Command started event occurs after ssn is encrypted
        expect(started_event.command["query"]["ssn"]["$eq"]).to be_ciphertext
        expect(started_event.command["update"]["ssn"]).to be_ciphertext

        # Command succeeded event occurs before ssn is decrypted
        expect(succeeded_event.reply["value"]["ssn"]).to be_ciphertext
      end

      it_behaves_like 'it has a non-encrypted key_vault_client'
    end

    describe '#find_one_and_update' do
      let(:command_name) { 'findAndModify' }

      before do
        encryption_client['users'].find_one_and_update(
          { ssn: ssn },
          { ssn: '555-555-5555' }
        )
      end

      it 'has encrypted data in command monitoring' do

        # Command started event occurs after ssn is encrypted
        expect(started_event.command["query"]["ssn"]["$eq"]).to be_ciphertext
        expect(started_event.command["update"]["ssn"]).to be_ciphertext

        # Command succeeded event occurs before ssn is decrypted
        expect(succeeded_event.reply["value"]["ssn"]).to be_ciphertext
      end

      it_behaves_like 'it has a non-encrypted key_vault_client'
    end

    describe '#replace_one' do
      let(:command_name) { 'update' }

      before do
        encryption_client['users'].replace_one(
          { ssn: ssn },
          { ssn: '555-555-5555' }
        )
      end

      it 'has encrypted data in command monitoring' do
        # Command started event occurs after ssn is encrypted
        # Command succeeded event does not contain any data to be decrypted
        expect(started_event.command["updates"].first["q"]["ssn"]["$eq"]).to be_ciphertext
        expect(started_event.command["updates"].first["u"]["ssn"]).to be_ciphertext
      end

      it_behaves_like 'it has a non-encrypted key_vault_client'
    end

    describe '#update_one' do
      let(:command_name) { 'update' }

      before do
        encryption_client['users'].replace_one({ ssn: ssn }, { ssn: '555-555-5555' })
      end

      it 'has encrypted data in command monitoring' do
        # Command started event occurs after ssn is encrypted
        # Command succeeded event does not contain any data to be decrypted
        expect(started_event.command["updates"].first["q"]["ssn"]["$eq"]).to be_ciphertext
        expect(started_event.command["updates"].first["u"]["ssn"]).to be_ciphertext
      end

      it_behaves_like 'it has a non-encrypted key_vault_client'
    end

    describe '#update_many' do
      let(:command_name) { 'update' }

      before do
        # update_many does not support replacement-style updates
        encryption_client['users'].update_many({ ssn: ssn }, { "$inc" => { :age => 1 } })
      end

      it 'has encrypted data in command monitoring' do
        # Command started event occurs after ssn is encrypted
        # Command succeeded event does not contain any data to be decrypted
        expect(started_event.command["updates"].first["q"]["ssn"]["$eq"]).to be_ciphertext
      end

      it_behaves_like 'it has a non-encrypted key_vault_client'
    end
  end

  describe '#insert_one' do
    let(:command_name) { 'insert' }

    before do
      encryption_client['users'].insert_one(ssn: ssn)
    end

    it 'has encrypted data in command monitoring' do
      # Command started event occurs after ssn is encrypted
      # Command succeeded event does not contain any data to be decrypted
      expect(started_event.command["documents"].first["ssn"]).to be_ciphertext
    end

    it_behaves_like 'it has a non-encrypted key_vault_client'
  end
end
