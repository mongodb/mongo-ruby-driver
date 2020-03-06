require 'spec_helper'

describe 'Batch writes with auto-encryption enabled' do
  require_libmongocrypt
  require_enterprise
  min_server_fcv '4.2'

  include_context 'define shared FLE helpers'
  include_context 'with local kms_providers'

  let(:subscriber) { EventSubscriber.new }

  let(:client) do
    new_local_client(
      SpecConfig.instance.addresses,
      SpecConfig.instance.test_options.merge(
        auto_encryption_options: {
          kms_providers: kms_providers,
          key_vault_namespace: key_vault_namespace,
          schema_map: { "auto_encryption.users" => schema_map },
        },
        database: 'auto_encryption'
      ),
    ).tap do |client|
      client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
    end
  end

  before do
    authorized_client.use('auto_encryption')['users'].drop

    authorized_client.use('admin')['datakeys'].drop
    authorized_client.use('admin')['datakeys'].insert_one(data_key)
  end

  let(:size_limit) { Mongo::Server::ConnectionBase::REDUCED_MAX_BSON_SIZE }

  context 'using BulkWrite' do
    let(:collection) { client['users'] }

    let(:bulk_write) do
      Mongo::BulkWrite.new(collection, requests, {})
    end

    shared_examples 'a functioning encrypted BulkWrite' do |options={}|
      num_writes = options[:num_writes]

      before do
        bulk_write.execute
      end

      it 'executes an encrypted bulk write' do
        documents = authorized_client.use(:auto_encryption)[:users].find
        ssns = documents.map { |doc| doc['ssn'] }
        expect(ssns).to all(be_ciphertext)
      end

      it 'executes the correct number of writes' do
        command_succeeded_events = subscriber.succeeded_events.select do |event|
          event.command_name == 'insert'
        end

        expect(command_succeeded_events.length).to eq(num_writes)
      end
    end

    context 'with insert operations' do
      context 'when total request size does not exceed 2MiB' do
        let(:requests) do
          [
            { insert_one: { ssn: 'a' * (size_limit/2) } },
            { insert_one: { ssn: 'a' * (size_limit/2) } }
          ]
        end

        it_behaves_like 'a functioning encrypted BulkWrite', num_writes: 1
      end

      context 'when each operation is smaller than 2MiB, but the total request size is greater than 2MiB' do
        let(:requests) do
          [
            { insert_one: { ssn: 'a' * (size_limit - 2000) } },
            { insert_one: { ssn: 'a' * (size_limit - 2000) } }
          ]
        end

        it_behaves_like 'a functioning encrypted BulkWrite', num_writes: 2
      end

      context 'when each operation is larger than 2MiB' do
        let(:requests) do
          [
            { insert_one: { ssn: 'a' * (size_limit * 2) } },
            { insert_one: { ssn: 'a' * (size_limit * 2) } }
          ]
        end

        it_behaves_like 'a functioning encrypted BulkWrite', num_writes: 2
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
          end.to raise_error(Mongo::Error::MaxBSONSize, /maximum allowed size: 16777216 bytes/)
        end
      end
    end

    context 'with update operations' do
      before do
        client['users'].insert_one(_id: 1)
        client['users'].insert_one(_id: 2)
      end

      # context 'when total request size does not exceed 2MiB' do
      #   let(:requests) do
      #     [
      #       { update_one: { filter: { _id: 1 }, update: { ssn: 'a' * (size_limit/10) } } },
      #       { update_one: { filter: { _id: 2 }, update: { ssn: 'a' * (size_limit/10) } } },
      #     ]
      #   end

      #   it_behaves_like 'a functioning encrypted BulkWrite', num_writes: 1
      # end

      context 'when each operation is smaller than 2MiB, but the total request size is greater than 2MiB' do
        let(:requests) do
          [
            { update_one: { filter: { _id: 1 }, update: { ssn: 'a' * (size_limit - 2000) } } },
            { update_one: { filter: { _id: 2 }, update: { ssn: 'a' * (size_limit - 2000) } } },
          ]
        end

        it_behaves_like 'a functioning encrypted BulkWrite', num_writes: 2
      end

      context 'when each operation is larger than 2MiB' do
        let(:requests) do
          [
            { update_one: { filter: { _id: 1 }, update: { ssn: 'a' * (size_limit * 2) } } },
            { update_one: { filter: { _id: 2 }, update: { ssn: 'a' * (size_limit * 2) } } },
          ]
        end

        it_behaves_like 'a functioning encrypted BulkWrite', num_writes: 2
      end

      context 'when one operation is larger than 16MiB' do
        let(:requests) do
          [
            { update_one: { filter: { _id: 1 }, update: { ssn: 'a' * (Mongo::Server::ConnectionBase::DEFAULT_MAX_BSON_OBJECT_SIZE - 100) } } },
            { update_one: { filter: { _id: 2 }, update: { ssn: 'a' * size_limit } } },
          ]
        end

        it 'raises an exception' do
          expect do
            bulk_write.execute
          end.to raise_error(Mongo::Error::MaxBSONSize, /maximum allowed size: 16777216 bytes/)
        end
      end
    end

    context 'with delete operations' do
      # context 'when total request size does not exceed 2MiB' do
      #   before do
      #     client['users'].insert_one(ssn: 'a' * (size_limit/10))
      #     client['users'].insert_one(ssn: 'b' * (size_limit/10))
      #   end

      #   let(:requests) do
      #     [
      #       { delete_one: { filter: { ssn: 'a' * (size_limit/10) } } },
      #       { delete_one: { filter: { ssn: 'b' * (size_limit/10) } } }
      #     ]
      #   end

      #   it_behaves_like 'a functioning encrypted BulkWrite', num_writes: 1
      # end

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

        it_behaves_like 'a functioning encrypted BulkWrite', num_writes: 2
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

        it_behaves_like 'a functioning encrypted BulkWrite', num_writes: 2
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
          end.to raise_error(Mongo::Error::MaxBSONSize, /maximum allowed size: 16777216 bytes/)
        end
      end
    end

    context 'with insert, update, and delete operations' do

    end
  end

  context '#insert_many' do

  end

  context '#update_many' do

  end

  context '#delete_many' do

  end
end
