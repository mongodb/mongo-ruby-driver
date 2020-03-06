require 'spec_helper'

describe 'Bulk insert' do
  include PrimarySocket

  let(:fail_point_base_command) do
    { 'configureFailPoint' => "failCommand" }
  end

  let(:collection_name) { 'bulk_insert_spec' }
  let(:collection) { authorized_client[collection_name] }

  describe 'inserted_ids' do
    before do
      collection.delete_many
    end

    context 'success' do
      it 'returns one insert_id as array' do
        result = collection.insert_many([
          {:_id => 9},
        ])
        expect(result.inserted_ids).to eql([9])
      end
    end

    context 'error on first insert' do
      it 'is an empty array' do
        collection.insert_one(:_id => 9)
        begin
          result = collection.insert_many([
            {:_id => 9},
          ])
          fail 'Should have raised'
        rescue Mongo::Error::BulkWriteError => e
          expect(e.result['inserted_ids']).to eql([])
        end
      end
    end

    context 'error on third insert' do
      it 'is an array of the first two ids' do
        collection.insert_one(:_id => 9)
        begin
          result = collection.insert_many([
            {:_id => 7},
            {:_id => 8},
            {:_id => 9},
          ])
          fail 'Should have raised'
        rescue Mongo::Error::BulkWriteError => e
          expect(e.result['inserted_ids']).to eql([7, 8])
        end
      end
    end

    context 'entire operation fails' do
      min_server_fcv '4.0'
      require_topology :single, :replica_set

      it 'is an empty array' do
        collection.client.use(:admin).command(fail_point_base_command.merge(
          :mode => {:times => 1},
          :data => {:failCommands => ['insert'], errorCode: 100}))
        begin
          result = collection.insert_many([
            {:_id => 7},
            {:_id => 8},
            {:_id => 9},
          ])
          fail 'Should have raised'
        rescue Mongo::Error => e
          result = e.send(:instance_variable_get, '@result')
          expect(result).to be_a(Mongo::Operation::Insert::BulkResult)
          expect(result.inserted_ids).to eql([])
        end
      end
    end
  end

  context 'with auto-encryption enabled' do
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

    let(:_2mib) { 2097152 }

    shared_examples 'a functioning encrypted bulk insert' do |options={}|
      num_writes = options[:num_writes]

      let!(:result) do
        client[:users].insert_many(documents)
      end

      it 'executes an encrypted bulk write' do
        documents = authorized_client
          .use(:auto_encryption)[:users]
          .find(_id: result.inserted_ids)

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

    before do
      client.use(:auto_encryption)[:users].drop
    end

    context 'when total request size does not exceed 2MiB' do
      let(:documents) do
        [
          { ssn: 'a' * (_2mib/2) },
          { ssn: 'a' * (_2mib/2) }
        ]
      end

      it_behaves_like 'a functioning encrypted bulk insert', num_writes: 1
    end

    context 'when each operation is smaller than 2MiB, but the total request size is greater than 2MiB' do
      let(:documents) do
        [
          { ssn: 'a' * (_2mib - 2000) },
          { ssn: 'a' * (_2mib - 2000) }
        ]
      end

      it_behaves_like 'a functioning encrypted bulk insert', num_writes: 2
    end

    context 'when each operation is larger than 2MiB' do
      let(:documents) do
        [
          { ssn: 'a' * (_2mib * 1.2) },
          { ssn: 'a' * (_2mib * 1.2) }
        ]

        it_behaves_like 'a functioning encrypted bulk insert', num_writes: 2
      end
    end
  end
end
