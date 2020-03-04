require 'spec_helper'

describe 'Client-Side Encryption' do
  describe 'Prose tests: BSON size limits and batch splitting' do
    require_libmongocrypt
    include_context 'define shared FLE helpers'

    let(:subscriber) { EventSubscriber.new }

    let(:client) do
      new_local_client(
        SpecConfig.instance.addresses,
        SpecConfig.instance.test_options
      )
    end

    let(:json_schema) do
      BSON::ExtJSON.parse(File.read('spec/support/crypt/limits/limits-schema.json'))
    end

    let(:client_encrypted) do
      new_local_client(
        SpecConfig.instance.addresses,
        SpecConfig.instance.test_options.merge(
          auto_encryption_options: {
            kms_providers: {
              local: { key: local_master_key },
            },
            key_vault_namespace: 'admin.datakeys',
          },
          database: :db,
        )
      ).tap do |client|
        client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
      end
    end

    let(:limits_doc) do
      BSON::ExtJSON.parse(File.read('spec/support/crypt/limits/limits-doc.json'))
    end

    before do
      client.use(:db)[:coll].drop
      client.use(:db)[:coll,
        {
          'validator' => { '$jsonSchema' => json_schema }
        }
      ].create

      client.use(:admin)[:datakeys].drop
      client.use(:admin)[:datakeys].insert_one(
        BSON::ExtJSON.parse(File.read('spec/support/crypt/limits/limits-key.json'))
      )
    end

    # context 'when document is under 2MiB limit' do
    #   it 'can perform insert_one using the encrypted client' do
    #     document = {
    #       _id: "under_2mib",
    #       unencrypted: 'a' * 2096980
    #     }

    #     result = client_encrypted[:coll].insert_one(document)

    #     expect(result).to be_ok
    #   end
    # end

    # context 'when document is under the 16MiB size limit' do
    #   it 'can perform insert_one using the encrypted client' do
    #     result = client_encrypted[:coll].insert_one(
    #       {
    #         _id: "encryption_exceeds_2mi",
    #         unencrypted: 'a' * (2096980 - 2000)
    #       }.merge(
    #         BSON::ExtJSON.parse(File.read('spec/support/crypt/limits/limits-doc.json'))
    #       )
    #     )

    #     expect(result).to be_ok
    #   end
    # end

    # context 'when there are multiple documents, but neither is greater than 2MiB in size' do
    #   it 'can perform bulk_insert using the encrypted client' do
    #     bulk_write = Mongo::BulkWrite.new(
    #       client_encrypted[:coll],
    #       [
    #         { insert_one: { _id: 'over_2mib_1', unencrypted: 'a' * 2096892 } },
    #         { insert_one: { _id: 'over_2mib_2', unencrypted: 'a' * 2096892 } },
    #       ]
    #     )

    #     result = bulk_write.execute
    #     expect(result.inserted_count).to eq(2)

    #     command_succeeded_events = subscriber.succeeded_events.select do |event|
    #       event.command_name == 'insert'
    #     end

    #     expect(command_succeeded_events.length).to eq(2)
    #   end
    # end

    context 'when the document falls under the maxBSONObjectSize limit' do
      it 'inserts the document' do
        result = client_encrypted[:coll].insert_one(
          _id: "under_16mib",
          unencrypted: "a" * (16777216 - 1000000)
        )

        expect(result).to be_ok
      end
    end

    context 'when the document exceeds maxBSONObjectSize limit' do
      it 'raises an exception when attempting to insert the document' do
        expect do
          client_encrypted[:coll].insert_one(
            limits_doc.merge(
              _id: "encryption_exceeds_16mib",
              unencrypted: "a" * (16777216 - 2000)
            )
          )
        end.to raise_error(Mongo::Error::MaxBSONSize, /Document exceeds allowed max BSON size/)
      end
    end
  end
end
