# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'Bulk writes with auto-encryption enabled' do
  require_libmongocrypt
  require_enterprise
  min_server_fcv '4.2'

  include_context 'define shared FLE helpers'
  include_context 'with local kms_providers'

  let(:subscriber) { Mrss::EventSubscriber.new }

  let(:client) do
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
        database: 'auto_encryption'
      ),
    ).tap do |client|
      client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
    end
  end

  let(:size_limit) { Mongo::Server::ConnectionBase::REDUCED_MAX_BSON_SIZE }

  before do
    authorized_client.use('auto_encryption')['users'].drop

    key_vault_collection.drop
    key_vault_collection.insert_one(data_key)
  end

  let(:command_succeeded_events) do
    subscriber.succeeded_events.select do |event|
      event.command_name == command_name
    end
  end

  shared_examples 'a functioning encrypted bulk write' do |options={}|
    num_writes = options[:num_writes]

    before do
      perform_bulk_write
    end

    it 'executes an encrypted bulk write' do
      documents = authorized_client.use('auto_encryption')['users'].find
      ssns = documents.map { |doc| doc['ssn'] }
      expect(ssns).to all(be_ciphertext)
    end

    it 'executes the correct number of writes' do
      expect(command_succeeded_events.length).to eq(num_writes)
    end
  end

  context 'using BulkWrite' do
    let(:collection) { client['users'] }
    let(:bulk_write) { Mongo::BulkWrite.new(collection, requests, {}) }
    let(:perform_bulk_write) { bulk_write.execute }

    context 'with insert operations' do
      let(:command_name) { 'insert' }

      context 'when total request size does not exceed 2MiB' do
        let(:requests) do
          [
            { insert_one: { ssn: 'a' * (size_limit/2) } },
            { insert_one: { ssn: 'a' * (size_limit/2) } }
          ]
        end

        it_behaves_like 'a functioning encrypted bulk write', num_writes: 1
      end

      context 'when each operation is smaller than 2MiB, but the total request size is greater than 2MiB' do
        let(:requests) do
          [
            { insert_one: { ssn: 'a' * (size_limit - 2000) } },
            { insert_one: { ssn: 'a' * (size_limit - 2000) } }
          ]
        end

        it_behaves_like 'a functioning encrypted bulk write', num_writes: 2
      end

      context 'when each operation is larger than 2MiB' do
        let(:requests) do
          [
            { insert_one: { ssn: 'a' * (size_limit * 2) } },
            { insert_one: { ssn: 'a' * (size_limit * 2) } }
          ]
        end

        it_behaves_like 'a functioning encrypted bulk write', num_writes: 2
      end

      context 'when one operation is larger than 16MiB' do
        let(:requests) do
          [
            { insert_one: { ssn: 'a' * (Mongo::Server::ConnectionBase::DEFAULT_MAX_BSON_OBJECT_SIZE + 1000) } },
            { insert_one: { ssn: 'a' * size_limit } }
          ]
        end

        it 'raises an exception' do
          expect do
            bulk_write.execute
          end.to raise_error(Mongo::Error::MaxBSONSize, /The document exceeds maximum allowed BSON object size after serialization/)
        end
      end
    end

    context 'with update operations' do
      let(:command_name) { 'update' }

      before do
        client['users'].insert_one(_id: 1)
        client['users'].insert_one(_id: 2)
      end

      context 'when total request size does not exceed 2MiB' do
        let(:requests) do
          [
            { replace_one: { filter: { _id: 1 }, replacement: { ssn: 'a' * (size_limit/2) } } },
            { replace_one: { filter: { _id: 2 }, replacement: { ssn: 'a' * (size_limit/2) } } },
          ]
        end

        it_behaves_like 'a functioning encrypted bulk write', num_writes: 1
      end

      context 'when each operation is smaller than 2MiB, but the total request size is greater than 2MiB' do
        let(:requests) do
          [
            { replace_one: { filter: { _id: 1 }, replacement: { ssn: 'a' * (size_limit - 2000) } } },
            { replace_one: { filter: { _id: 2 }, replacement: { ssn: 'a' * (size_limit - 2000) } } },
          ]
        end

        it_behaves_like 'a functioning encrypted bulk write', num_writes: 2
      end

      context 'when each operation is larger than 2MiB' do
        let(:requests) do
          [
            { replace_one: { filter: { _id: 1 }, replacement: { ssn: 'a' * (size_limit * 2) } } },
            { replace_one: { filter: { _id: 2 }, replacement: { ssn: 'a' * (size_limit * 2) } } },
          ]
        end

        it_behaves_like 'a functioning encrypted bulk write', num_writes: 2
      end

      context 'when one operation is larger than 16MiB' do
        let(:requests) do
          [
            { replace_one: { filter: { _id: 1 }, replacement: { ssn: 'a' * (Mongo::Server::ConnectionBase::DEFAULT_MAX_BSON_OBJECT_SIZE) } } },
            { replace_one: { filter: { _id: 2 }, replacement: { ssn: 'a' * size_limit } } },
          ]
        end

        before do
          expect(requests.first.to_bson.length).to be > Mongo::Server::ConnectionBase::DEFAULT_MAX_BSON_OBJECT_SIZE
        end

        it 'raises an exception' do
          expect do
            bulk_write.execute
          end.to raise_error(Mongo::Error::MaxBSONSize, /The document exceeds maximum allowed BSON object size after serialization/)
        end
      end
    end

    context 'with delete operations' do
      let(:command_name) { 'delete' }

      context 'when total request size does not exceed 2MiB' do
        before do
          client['users'].insert_one(ssn: 'a' * (size_limit/2))
          client['users'].insert_one(ssn: 'b' * (size_limit/2))
        end

        let(:requests) do
          [
            { delete_one: { filter: { ssn: 'a' * (size_limit/2) } } },
            { delete_one: { filter: { ssn: 'b' * (size_limit/2) } } }
          ]
        end

        it 'performs one delete' do
          bulk_write.execute

          documents = authorized_client.use('auto_encryption')['users'].find.to_a
          expect(documents.length).to eq(0)
          expect(command_succeeded_events.length).to eq(1)
        end
      end

      context 'when each operation is smaller than 2MiB, but the total request size is greater than 2MiB' do
        before do
          client['users'].insert_one(ssn: 'a' * (size_limit - 2000))
          client['users'].insert_one(ssn: 'b' * (size_limit - 2000))
        end

        let(:requests) do
          [
            { delete_one: { filter: { ssn: 'a' * (size_limit - 2000) } } },
            { delete_one: { filter: { ssn: 'b' * (size_limit - 2000) } } }
          ]
        end

        it 'performs two deletes' do
          bulk_write.execute

          documents = authorized_client.use('auto_encryption')['users'].find.to_a
          expect(documents.length).to eq(0)
          expect(command_succeeded_events.length).to eq(2)
        end
      end

      context 'when each operation is larger than 2MiB' do
        before do
          client['users'].insert_one(ssn: 'a' * (size_limit * 2))
          client['users'].insert_one(ssn: 'b' * (size_limit * 2))
        end

        let(:requests) do
          [
            { delete_one: { filter: { ssn: 'a' * (size_limit * 2) } } },
            { delete_one: { filter: { ssn: 'b' * (size_limit * 2) } } }
          ]
        end

        it 'performs two deletes' do
          bulk_write.execute

          documents = authorized_client.use('auto_encryption')['users'].find.to_a
          expect(documents.length).to eq(0)
          expect(command_succeeded_events.length).to eq(2)
        end
      end

      context 'when one operation is larger than 16MiB' do
        let(:requests) do
          [
            { delete_one: { filter: { ssn: 'a' * (Mongo::Server::ConnectionBase::DEFAULT_MAX_BSON_OBJECT_SIZE + 1000) } } },
            { delete_one: { filter: { ssn: 'b' * (size_limit * 2) } } }
          ]
        end

        it 'raises an exception' do
          expect do
            bulk_write.execute
          end.to raise_error(Mongo::Error::MaxBSONSize, /The document exceeds maximum allowed BSON object size after serialization/)
        end
      end
    end

    context 'with insert, update, and delete operations' do
      context 'when total request size does not exceed 2MiB' do
        let(:requests) do
          [
            { insert_one: { _id: 1, ssn: 'a' * (size_limit/3) } },
            { replace_one: { filter: { _id: 1 }, replacement: { ssn: 'b' * (size_limit/3) } } },
            { delete_one: { filter: { ssn: 'b' * (size_limit/3) } } }
          ]
        end

        it 'successfully performs the bulk write' do
          bulk_write.execute

          documents = authorized_client.use('auto_encryption')['users'].find.to_a
          expect(documents.length).to eq(0)
        end

        # Bulk writes with different types of operations should
        it 'performs 1 insert, 1 update, and 1 delete' do
          bulk_write.execute

          command_succeeded_events = subscriber.succeeded_events

          inserts = command_succeeded_events.select { |event| event.command_name == 'insert' }
          updates = command_succeeded_events.select { |event| event.command_name == 'update' }
          deletes = command_succeeded_events.select { |event| event.command_name == 'delete' }

          expect(inserts.length).to eq(1)
          expect(updates.length).to eq(1)
          expect(deletes.length).to eq(1)
        end
      end
    end
  end

  context '#insert_many' do
    let(:perform_bulk_write) do
      client['users'].insert_many(documents)
    end

    let(:command_name) { 'insert' }

    context 'when total request size does not exceed 2MiB' do
      let(:documents) do
        [
          { ssn: 'a' * (size_limit/2) },
          { ssn: 'a' * (size_limit/2) },
        ]
      end

      it_behaves_like 'a functioning encrypted bulk write', num_writes: 1
    end

    context 'when each operation is smaller than 2MiB, but the total request size is greater than 2MiB' do
      let(:documents) do
        [
          { ssn: 'a' * (size_limit - 2000) },
          { ssn: 'a' * (size_limit - 2000) },
        ]
      end

      it_behaves_like 'a functioning encrypted bulk write', num_writes: 2
    end

    context 'when each operation is larger than 2MiB' do
      let(:documents) do
        [
          { ssn: 'a' * (size_limit * 2) },
          { ssn: 'a' * (size_limit * 2) },
        ]
      end

      it_behaves_like 'a functioning encrypted bulk write', num_writes: 2
    end

    context 'when one operation is larger than 16MiB' do
      let(:documents) do
        [
          { ssn: 'a' * (Mongo::Server::ConnectionBase::DEFAULT_MAX_BSON_OBJECT_SIZE + 1000) },
          { ssn: 'a' * size_limit },
        ]
      end

      it 'raises an exception' do
        expect do
          perform_bulk_write
        end.to raise_error(Mongo::Error::MaxBSONSize, /The document exceeds maximum allowed BSON object size after serialization/)
      end
    end
  end
end
