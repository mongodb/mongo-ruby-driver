# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'Client-Side Encryption' do
  describe 'Prose tests: BSON size limits and batch splitting' do
    require_libmongocrypt
    require_enterprise
    min_server_fcv '4.2'

    include_context 'define shared FLE helpers'

    let(:subscriber) { Mrss::EventSubscriber.new }

    let(:client) do
      authorized_client.use('db')
    end

    let(:json_schema) do
      BSON::ExtJSON.parse(File.read('spec/support/crypt/limits/limits-schema.json'))
    end

    let(:limits_doc) do
      BSON::ExtJSON.parse(File.read('spec/support/crypt/limits/limits-doc.json'))
    end

    let(:client_encrypted) do
      new_local_client(
        SpecConfig.instance.addresses,
        SpecConfig.instance.test_options.merge(
          auto_encryption_options: {
            kms_providers: {
              local: { key: local_master_key },
            },
            key_vault_namespace: 'keyvault.datakeys',
            # Spawn mongocryptd on non-default port for sharded cluster tests
            extra_options: extra_options,
          },
          database: 'db',
        )
      ).tap do |client|
        client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
      end
    end

    before do
      client['coll'].drop
      client['coll',
        {
          'validator' => { '$jsonSchema' => json_schema }
        }
      ].create

      key_vault_collection = client.use('keyvault')['datakeys', write_concern: { w: :majority }]

      key_vault_collection.drop
      key_vault_collection.insert_one(
        BSON::ExtJSON.parse(File.read('spec/support/crypt/limits/limits-key.json'))
      )
    end

    let(:_2mib) { 2097152 }
    let(:_16mib) { 16777216 }

    context 'when a single, unencrypted document is larger than 2MiB' do
      it 'can perform insert_one using the encrypted client' do
        document = {
          _id: "over_2mib_under_16mib",
          unencrypted: 'a' * _2mib
        }

        result = client_encrypted['coll'].insert_one(document)

        expect(result).to be_ok
      end
    end

    context 'when a single encrypted document is larger than 2MiB' do
      it 'can perform insert_one using the encrypted client' do
        result = client_encrypted['coll'].insert_one(
          limits_doc.merge(
            _id: "encryption_exceeds_2mi",
            unencrypted: 'a' * (_2mib - 2000)
          )
        )

        expect(result).to be_ok
      end
    end

    context 'when bulk inserting two unencrypted documents under 2MiB' do
      it 'can perform bulk insert using the encrypted client' do
        bulk_write = Mongo::BulkWrite.new(
          client_encrypted['coll'],
          [
            { insert_one: { _id: 'over_2mib_1', unencrypted: 'a' * _2mib } },
            { insert_one: { _id: 'over_2mib_2', unencrypted: 'a' * _2mib } },
          ]
        )

        result = bulk_write.execute
        expect(result.inserted_count).to eq(2)

        command_succeeded_events = subscriber.succeeded_events.select do |event|
          event.command_name == 'insert'
        end

        expect(command_succeeded_events.length).to eq(2)
      end
    end

    context 'when bulk deletes two unencrypted documents under 2MiB' do
      it 'can perform bulk delete using the encrypted client' do
        # Insert documents that we can match and delete later
        bulk_write = Mongo::BulkWrite.new(
          client_encrypted['coll'],
          [
            { insert_one: { _id: 'over_2mib_1', unencrypted: 'a' * _2mib } },
            { insert_one: { _id: 'over_2mib_2', unencrypted: 'a' * _2mib } },
          ]
        )

        result = bulk_write.execute
        expect(result.inserted_count).to eq(2)

        command_succeeded_events = subscriber.succeeded_events.select do |event|
          event.command_name == 'insert'
        end

        expect(command_succeeded_events.length).to eq(2)
      end
    end

    context 'when bulk inserting two encrypted documents under 2MiB' do
      it 'can perform bulk_insert using the encrypted client' do
        bulk_write = Mongo::BulkWrite.new(
          client_encrypted['coll'],
          [
            {
              insert_one: limits_doc.merge(
                _id: "encryption_exceeds_2mib_1",
                unencrypted: 'a' * (_2mib - 2000)
              )
            },
            {
              insert_one: limits_doc.merge(
                _id: 'encryption_exceeds_2mib_2',
                unencrypted: 'a' * (_2mib - 2000)
              )
            },
          ]
        )

        result = bulk_write.execute
        expect(result.inserted_count).to eq(2)

        command_succeeded_events = subscriber.succeeded_events.select do |event|
          event.command_name == 'insert'
        end

        expect(command_succeeded_events.length).to eq(2)
      end
    end

    context 'when a single document is just smaller than 16MiB' do
      it 'can perform insert_one using the encrypted client' do
        result = client_encrypted['coll'].insert_one(
          _id: "under_16mib",
          unencrypted: "a" * (_16mib - 2000)
        )

        expect(result).to be_ok
      end
    end

    context 'when an encrypted document is greater than the 16MiB limit' do
      it 'raises an exception when attempting to insert the document' do
        expect do
          client_encrypted['coll'].insert_one(
            limits_doc.merge(
              _id: "encryption_exceeds_16mib",
              unencrypted: "a" * (16*1024*1024 + 500*1024),
            )
          )
        end.to raise_error(Mongo::Error::MaxBSONSize, /The document exceeds maximum allowed BSON object size after serialization/)
      end
    end
  end
end
