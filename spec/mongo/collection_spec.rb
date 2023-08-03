# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Collection do

  let(:subscriber) { Mrss::EventSubscriber.new }

  let(:client) do
    authorized_client.tap do |client|
      client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
    end
  end

  let(:authorized_collection) { client['collection_spec'] }

  before do
    authorized_client['collection_spec'].drop
  end

  describe '#==' do

    let(:database) do
      Mongo::Database.new(authorized_client, :test)
    end

    let(:collection) do
      described_class.new(database, :users)
    end

    context 'when the names are the same' do

      context 'when the databases are the same' do

        let(:other) do
          described_class.new(database, :users)
        end

        it 'returns true' do
          expect(collection).to eq(other)
        end
      end

      context 'when the databases are not the same' do

        let(:other_db) do
          Mongo::Database.new(authorized_client, :testing)
        end

        let(:other) do
          described_class.new(other_db, :users)
        end

        it 'returns false' do
          expect(collection).to_not eq(other)
        end
      end

      context 'when the options are the same' do

        let(:other) do
          described_class.new(database, :users)
        end

        it 'returns true' do
          expect(collection).to eq(other)
        end
      end

      context 'when the options are not the same' do

        let(:other) do
          described_class.new(database, :users, :capped => true)
        end

        it 'returns false' do
          expect(collection).to_not eq(other)
        end
      end
    end

    context 'when the names are not the same' do

      let(:other) do
        described_class.new(database, :sounds)
      end

      it 'returns false' do
        expect(collection).to_not eq(other)
      end
    end

    context 'when the object is not a collection' do

      it 'returns false' do
        expect(collection).to_not eq('test')
      end
    end
  end

  describe '#initialize' do

    let(:client) do
      new_local_client(SpecConfig.instance.addresses,
        SpecConfig.instance.test_options.merge(monitoring_io: false))
    end

    let(:database) { client.database }

    context 'write concern given in :write option' do
      let(:collection) do
        Mongo::Collection.new(database, 'foo', write: {w: 1})
      end

      it 'stores write concern' do
        expect(collection.write_concern).to be_a(Mongo::WriteConcern::Acknowledged)
        expect(collection.write_concern.options).to eq(w: 1)
      end

      it 'stores write concern under :write' do
        expect(collection.options[:write]).to eq(w: 1)
        expect(collection.options[:write_concern]).to be nil
      end
    end

    context 'write concern given in :write_concern option' do
      let(:collection) do
        Mongo::Collection.new(database, 'foo', write_concern: {w: 1})
      end

      it 'stores write concern' do
        expect(collection.write_concern).to be_a(Mongo::WriteConcern::Acknowledged)
        expect(collection.write_concern.options).to eq(w: 1)
      end

      it 'stores write concern under :write_concern' do
        expect(collection.options[:write_concern]).to eq(w: 1)
        expect(collection.options[:write]).to be nil
      end
    end

    context 'write concern given in both :write and :write_concern options' do
      context 'identical values' do

        let(:collection) do
          Mongo::Collection.new(database, 'foo',
            write: {w: 1}, write_concern: {w: 1})
        end

        it 'stores write concern' do
          expect(collection.write_concern).to be_a(Mongo::WriteConcern::Acknowledged)
          expect(collection.write_concern.options).to eq(w: 1)
        end

        it 'stores write concern under both options' do
          expect(collection.options[:write]).to eq(w: 1)
          expect(collection.options[:write_concern]).to eq(w: 1)
        end
      end

      context 'different values' do

        let(:collection) do
          Mongo::Collection.new(database, 'foo',
            write: {w: 1}, write_concern: {w: 2})
        end

        it 'raises an exception' do
          expect do
            collection
          end.to raise_error(ArgumentError, /If :write and :write_concern are both given, they must be identical/)
        end
      end
    end

=begin WriteConcern object support
    context 'when write concern is provided via a WriteConcern object' do

      let(:collection) do
        Mongo::Collection.new(database, 'foo', write_concern: wc)
      end

      let(:wc) { Mongo::WriteConcern.get(w: 2) }

      it 'stores write concern options in collection options' do
        expect(collection.options[:write_concern]).to eq(w: 2)
      end

      it 'caches write concern object' do
        expect(collection.write_concern).to be wc
      end
    end
=end
  end

  describe '#with' do

    let(:client) do
      new_local_client_nmio(SpecConfig.instance.addresses,
        SpecConfig.instance.test_options.merge(
          SpecConfig.instance.auth_options
      ))
    end

    let(:database) do
      Mongo::Database.new(client, SpecConfig.instance.test_db)
    end

    let(:collection) do
      database.collection('test-collection')
    end

    let(:new_collection) do
      collection.with(new_options)
    end

    context 'when new read options are provided' do

      let(:new_options) do
        { read: { mode: :secondary } }
      end

      it 'returns a new collection' do
        expect(new_collection).not_to be(collection)
      end

      it 'sets the new read options on the new collection' do
        expect(new_collection.read_preference).to eq(new_options[:read])
      end

      context 'when the client has a server selection timeout setting' do

        let(:client) do
          new_local_client(SpecConfig.instance.addresses,
            SpecConfig.instance.test_options.merge(server_selection_timeout: 2, monitoring_io: false))
        end

        it 'passes the the server_selection_timeout to the cluster' do
          expect(client.cluster.options[:server_selection_timeout]).to eq(client.options[:server_selection_timeout])
        end
      end

      context 'when the client has a read preference set' do

        let(:client) do
          authorized_client.with(client_options).tap do |client|
            expect(client.options[:read]).to eq(Mongo::Options::Redacted.new(
              mode: :primary_preferred))
            client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
          end
        end

        let(:client_options) do
          {
            read: { mode: :primary_preferred },
            monitoring_io: false,
          }
        end

        let(:new_options) do
          { read: { mode: :secondary } }
        end

        it 'sets the new read options on the new collection' do
          # This is strictly a Hash, not a BSON::Document like the client's
          # read preference.
          expect(new_collection.read_preference).to eq(mode: :secondary)
        end

        it 'duplicates the read option' do
          expect(new_collection.read_preference).not_to eql(client.read_preference)
        end

        context 'when reading from collection' do
          # Since we are requesting a secondary read, we need a replica set.
          require_topology :replica_set

          let(:client_options) do
            {read: { mode: :primary_preferred }}
          end

          shared_examples_for "uses collection's read preference when reading" do
            it "uses collection's read preference when reading" do
              expect do
                new_collection.find.to_a.count
              end.not_to raise_error

              event = subscriber.started_events.detect do |event|
                event.command['find']
              end
              actual_rp = event.command['$readPreference']
              expect(actual_rp).to eq(expected_read_preference)
            end
          end

          context 'post-OP_MSG server' do
            min_server_fcv '3.6'

            context 'standalone' do
              require_topology :single

              let(:expected_read_preference) do
                nil
              end

              it_behaves_like "uses collection's read preference when reading"
            end

            context 'RS, sharded' do
              require_topology :replica_set, :sharded

              let(:expected_read_preference) do
                {'mode' => 'secondary'}
              end

              it_behaves_like "uses collection's read preference when reading"
            end
          end

          context 'pre-OP-MSG server' do
            max_server_version '3.4'

            let(:expected_read_preference) do
              nil
            end

            it_behaves_like "uses collection's read preference when reading"
          end
        end
      end

      context 'when the client has a read preference and server selection timeout set' do

        let(:client) do
          new_local_client(SpecConfig.instance.addresses,
            SpecConfig.instance.test_options.merge(
              read: { mode: :primary_preferred },
              server_selection_timeout: 2,
              monitoring_io: false
          ))
        end

        it 'sets the new read options on the new collection' do
          expect(new_collection.read_preference).to eq(new_options[:read])
        end

        it 'passes the server_selection_timeout setting to the cluster' do
          expect(client.cluster.options[:server_selection_timeout]).to eq(client.options[:server_selection_timeout])
        end
      end
    end

    context 'when new write options are provided' do

      let(:new_options) do
        { write: { w: 5 } }
      end

      it 'returns a new collection' do
        expect(new_collection).not_to be(collection)
      end

      it 'sets the new write options on the new collection' do
        expect(new_collection.write_concern.options).to eq(Mongo::WriteConcern.get(new_options[:write]).options)
      end

      context 'when the client has a write concern set' do

        let(:client) do
          new_local_client(SpecConfig.instance.addresses,
            SpecConfig.instance.test_options.merge(
              write: INVALID_WRITE_CONCERN,
              monitoring_io: false,
          ))
        end

        it 'sets the new write options on the new collection' do
          expect(new_collection.write_concern.options).to eq(Mongo::WriteConcern.get(new_options[:write]).options)
        end

        context 'when client uses :write_concern and collection uses :write' do

          let(:client) do
            new_local_client(SpecConfig.instance.addresses,
              SpecConfig.instance.test_options.merge(
                write_concern: {w: 1},
                monitoring_io: false,
            ))
          end

          it 'uses :write from collection options only' do
            expect(new_collection.options[:write]).to eq(w: 5)
            expect(new_collection.options[:write_concern]).to be nil
          end
        end

        context 'when client uses :write and collection uses :write_concern' do

          let(:client) do
            new_local_client(SpecConfig.instance.addresses,
              SpecConfig.instance.test_options.merge(
                write: {w: 1},
                monitoring_io: false,
            ))
          end

          let(:new_options) do
            { write_concern: { w: 5 } }
          end

          it 'uses :write_concern from collection options only' do
            expect(new_collection.options[:write_concern]).to eq(w: 5)
            expect(new_collection.options[:write]).to be nil
          end
        end

        context 'when collection previously had :wrte_concern and :write is used with a different value' do

          let(:collection) do
            database.collection(:users, write_concern: {w: 2})
          end

          let(:new_options) do
            { write: { w: 5 } }
          end

          it 'uses the new option' do
            expect(new_collection.options[:write]).to eq(w: 5)
            expect(new_collection.options[:write_concern]).to be nil
          end
        end

        context 'when collection previously had :wrte and :write_concern is used with a different value' do

          let(:collection) do
            database.collection(:users, write: {w: 2})
          end

          let(:new_options) do
            { write_concern: { w: 5 } }
          end

          it 'uses the new option' do
            expect(new_collection.options[:write_concern]).to eq(w: 5)
            expect(new_collection.options[:write]).to be nil
          end
        end

        context 'when collection previously had :wrte_concern and :write is used with the same value' do

          let(:collection) do
            database.collection(:users, write_concern: {w: 2})
          end

          let(:new_options) do
            { write: { w: 2 } }
          end

          it 'uses the new option' do
            expect(new_collection.options[:write]).to eq(w: 2)
            expect(new_collection.options[:write_concern]).to be nil
          end
        end

        context 'when collection previously had :wrte and :write_concern is used with the same value' do

          let(:collection) do
            database.collection(:users, write: {w: 2})
          end

          let(:new_options) do
            { write_concern: { w: 2 } }
          end

          it 'uses the new option' do
            expect(new_collection.options[:write]).to be nil
            expect(new_collection.options[:write_concern]).to eq(w: 2)
          end
        end
      end
    end

    context 'when new read and write options are provided' do

      let(:new_options) do
        {
          read: { mode: :secondary },
          write: { w: 4}
        }
      end

      it 'returns a new collection' do
        expect(new_collection).not_to be(collection)
      end

      it 'sets the new read options on the new collection' do
        expect(new_collection.read_preference).to eq(new_options[:read])
      end

      it 'sets the new write options on the new collection' do
        expect(new_collection.write_concern.options).to eq(Mongo::WriteConcern.get(new_options[:write]).options)
      end

      context 'when the client has a server selection timeout setting' do

        let(:client) do
          new_local_client(SpecConfig.instance.addresses,
            SpecConfig.instance.test_options.merge(
              server_selection_timeout: 2,
              monitoring_io: false,
          ))
        end

        it 'passes the server_selection_timeout setting to the cluster' do
          expect(client.cluster.options[:server_selection_timeout]).to eq(client.options[:server_selection_timeout])
        end
      end

      context 'when the client has a read preference set' do

        let(:client) do
          new_local_client(SpecConfig.instance.addresses,
            SpecConfig.instance.test_options.merge(
              read: { mode: :primary_preferred },
              monitoring_io: false,
          ))
        end

        it 'sets the new read options on the new collection' do
          expect(new_collection.read_preference).to eq(new_options[:read])
          expect(new_collection.read_preference).not_to be(client.read_preference)
        end
      end
    end

    context 'when neither read nor write options are provided' do

      let(:new_options) do
        { some_option: 'invalid' }
      end

      it 'raises an error' do
        expect {
          new_collection
        }.to raise_exception(Mongo::Error::UnchangeableCollectionOption)
      end
    end
  end

  describe '#read_preference' do

    let(:collection) do
      described_class.new(authorized_client.database, :users, options)
    end

    let(:options) { {} }

    context 'when a read preference is set in the options' do

      let(:options) do
        { read: { mode: :secondary } }
      end

      it 'returns the read preference' do
        expect(collection.read_preference).to eq(options[:read])
      end
    end

    context 'when a read preference is not set in the options' do

      context 'when the database has a read preference set' do

        let(:client) do
          authorized_client.with(read: { mode: :secondary_preferred })
        end

        let(:collection) do
          described_class.new(client.database, :users, options)
        end

        it 'returns the database read preference' do
          expect(collection.read_preference).to eq(BSON::Document.new({ mode: :secondary_preferred }))
        end
      end

      context 'when the database does not have a read preference' do

        it 'returns nil' do
          expect(collection.read_preference).to be_nil
        end
      end
    end
  end

  describe '#server_selector' do

    let(:collection) do
      described_class.new(authorized_client.database, :users, options)
    end

    let(:options) { {} }

    context 'when a read preference is set in the options' do

      let(:options) do
        { read: { mode: :secondary } }
      end

      it 'returns the server selector for that read preference' do
        expect(collection.server_selector).to be_a(Mongo::ServerSelector::Secondary)
      end
    end

    context 'when a read preference is not set in the options' do

      context 'when the database has a read preference set' do

        let(:client) do
          authorized_client.with(read: { mode: :secondary_preferred })
        end

        let(:collection) do
          described_class.new(client.database, :users, options)
        end

        it 'returns the server selector for that read preference' do
          expect(collection.server_selector).to be_a(Mongo::ServerSelector::SecondaryPreferred)
        end
      end

      context 'when the database does not have a read preference' do

        it 'returns a primary server selector' do
          expect(collection.server_selector).to be_a(Mongo::ServerSelector::Primary)
        end
      end
    end
  end

  describe '#capped?' do

    let(:database) do
      authorized_client.database
    end

    context 'when the collection is capped' do

      let(:collection) do
        described_class.new(database, :specs, :capped => true, :size => 4096, :max => 512)
      end

      let(:collstats) do
        collection.aggregate([ {'$collStats' => { 'storageStats' => {} }} ]).first
      end

      let(:storage_stats) do
        collstats.fetch('storageStats', {})
      end

      before do
        authorized_client[:specs].drop
        collection.create
      end

      it 'returns true' do
        expect(collection).to be_capped
      end

      it "applies the options" do
        expect(storage_stats["capped"]).to be true
        expect(storage_stats["max"]).to eq(512)
        expect(storage_stats["maxSize"]).to eq(4096)
      end
    end

    context 'when the collection is not capped' do

      let(:collection) do
        described_class.new(database, :specs)
      end

      before do
        authorized_client[:specs].drop
        collection.create
      end

      it 'returns false' do
        expect(collection).to_not be_capped
      end
    end
  end

  describe '#inspect' do

    it 'includes the object id' do
      expect(authorized_collection.inspect).to include(authorized_collection.object_id.to_s)
    end

    it 'includes the namespace' do
      expect(authorized_collection.inspect).to include(authorized_collection.namespace)
    end
  end

  describe '#watch' do

    context 'when change streams can be tested' do
      require_wired_tiger
      min_server_fcv '3.6'
      require_topology :replica_set

      let(:change_stream) do
        authorized_collection.watch
      end

      let(:enum) do
        change_stream.to_enum
      end

      before do
        change_stream
        authorized_collection.insert_one(a: 1)
      end

      context 'when no options are provided' do

        context 'when the operation type is an insert' do

          it 'returns the change' do
            expect(enum.next[:fullDocument][:a]).to eq(1)
          end
        end

        context 'when the operation type is an update' do

          before do
            authorized_collection.update_one({ a: 1 }, { '$set' => { a: 2 } })
          end

          let(:change_doc) do
            enum.next
            enum.next
          end

          it 'returns the change' do
            expect(change_doc[:operationType]).to eq('update')
            expect(change_doc[:updateDescription][:updatedFields]).to eq('a' => 2)
          end
        end
      end

      context 'when options are provided' do

        context 'when full_document is updateLookup' do

          let(:change_stream) do
            authorized_collection.watch([], full_document: 'updateLookup').to_enum
          end

          before do
            authorized_collection.update_one({ a: 1 }, { '$set' => { a: 2 } })
          end

          let(:change_doc) do
            enum.next
            enum.next
          end

          it 'returns the change' do
            expect(change_doc[:operationType]).to eq('update')
            expect(change_doc[:fullDocument][:a]).to eq(2)
          end
        end

        context 'when batch_size is provided' do

          before do
            Thread.new do
              sleep 1
              authorized_collection.insert_one(a: 2)
              authorized_collection.insert_one(a: 3)
            end
          end

          let(:change_stream) do
            authorized_collection.watch([], batch_size: 2)
          end

          it 'returns the documents in the batch size specified' do
            expect(change_stream.instance_variable_get(:@cursor)).to receive(:get_more).once.and_call_original
            enum.next
          end
        end

        context 'when collation is provided' do

          before do
            authorized_collection.update_one({ a: 1 }, { '$set' => { a: 2 } })
          end

          let(:change_doc) do
            enum.next
          end

          let(:change_stream) do
            authorized_collection.watch([ { '$match' => { operationType: 'UPDATE'}}],
                                        collation: { locale: 'en_US', strength: 2 } ).to_enum
          end

          it 'returns the change' do
            expect(change_doc['operationType']).to eq('update')
            expect(change_doc['updateDescription']['updatedFields']['a']).to eq(2)
          end
        end
      end
    end

    context 'when the change stream is empty' do
      require_wired_tiger
      min_server_fcv '3.6'
      require_topology :replica_set

      context 'when setting the max_await_time_ms' do

        let(:change_stream) do
          authorized_collection.watch([], max_await_time_ms: 3000)
        end

        let(:enum) { change_stream.to_enum }

        it 'sets the option correctly' do
          expect(change_stream.instance_variable_get(:@cursor)).to receive(:get_more_operation).once.and_wrap_original do |m, *args, &block|
            m.call(*args).tap do |op|
              expect(op.max_time_ms).to eq(3000)
            end
          end
          enum.next
        end

        it "waits the appropriate amount of time" do
          start_time = Mongo::Utils.monotonic_time
          enum.try_next
          end_time = Mongo::Utils.monotonic_time

          expect(end_time-start_time).to be >= 3
        end
      end
    end
  end
end
