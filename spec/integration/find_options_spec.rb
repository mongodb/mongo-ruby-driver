# frozen_string_literal: true

require 'spec_helper'

describe 'Find operation options' do
  let(:subscriber) { Mrss::EventSubscriber.new }

  let(:seeds) do
    [ SpecConfig.instance.addresses.first ]
  end

  let(:client) do
    ClientRegistry.instance.new_local_client(
      seeds,
      SpecConfig.instance.test_options.merge(client_options)
    ).tap do |client|
      client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
    end
  end

  let(:collection) do
    client['find_options', collection_options]
  end

  let(:find_command) do
    subscriber.started_events.find { |cmd| cmd.command_name == 'find' }
  end

  before do
    ClientRegistry.instance.global_client('authorized')['find_options'].drop
    collection.insert_many([ { a: 1 }, { a: 2 }, { a: 3 } ])
  end

  describe 'collation' do
    let(:client_options) do
      {}
    end

    let(:collation) do
      { 'locale' => 'en_US' }
    end

    context 'when defined on the collection' do
      let(:collection_options) do
        { collation: collation }
      end

      it 'uses the collation defined on the collection' do
        collection.find.to_a
        expect(find_command.command['collation']).to be_nil
      end
    end

    context 'when defined on the operation' do
      let(:collection_options) do
        {}
      end

      it 'uses the collation defined on the collection' do
        collection.find({}, collation: collation).to_a
        expect(find_command.command['collation']).to eq(collation)
      end
    end

    context 'when defined on both collection and operation' do
      let(:collection_options) do
        { 'locale' => 'de_AT' }
      end

      it 'uses the collation defined on the collection' do
        collection.find({}, collation: collation).to_a
        expect(find_command.command['collation']).to eq(collation)
      end
    end
  end

  describe 'read concern' do
    let(:read_concern) do
      { 'level' => 'local' }
    end

    context 'when defined on the client' do
      let(:client_options) do
        { read_concern: read_concern }
      end

      let(:collection_options) do
        {}
      end

      it 'uses the read concern defined on the client' do
        collection.find.to_a
        expect(find_command.command['readConcern']).to eq(read_concern)
      end

      context 'when defined on the collection' do
        let(:collection_options) do
          { read_concern: { 'level' => 'majority' } }
        end

        it 'uses the read concern defined on the collection' do
          collection.find.to_a
          expect(find_command.command['readConcern']).to eq(collection_options[:read_concern])
        end

        context 'when defined on the operation' do
          let(:operation_read_concern) do
            { 'level' => 'available' }
          end

          it 'uses the read concern defined on the operation' do
            collection.find({}, read_concern: operation_read_concern).to_a
            expect(find_command.command['readConcern']).to eq(operation_read_concern)
          end
        end
      end

      context 'when defined on the operation' do
        let(:collection_options) do
          {}
        end

        let(:operation_read_concern) do
          { 'level' => 'available' }
        end

        it 'uses the read concern defined on the operation' do
          collection.find({}, read_concern: operation_read_concern).to_a
          expect(find_command.command['readConcern']).to eq(operation_read_concern)
        end
      end
    end

    context 'when defined on the collection' do
      let(:client_options) do
        {}
      end

      let(:collection_options) do
        { read_concern: { 'level' => 'majority' } }
      end

      it 'uses the read concern defined on the collection' do
        collection.find.to_a
        expect(find_command.command['readConcern']).to eq(collection_options[:read_concern])
      end

      context 'when defined on the operation' do
        let(:operation_read_concern) do
          { 'level' => 'available' }
        end

        it 'uses the read concern defined on the operation' do
          collection.find({}, read_concern: operation_read_concern).to_a
          expect(find_command.command['readConcern']).to eq(operation_read_concern)
        end
      end
    end
  end

  describe 'read preference' do
    require_topology :replica_set

    context 'when defined on the client' do
      let(:client_options) do
        { read: { mode: :secondary } }
      end

      let(:collection_options) do
        {}
      end

      it 'uses the read preference defined on the client' do
        collection.find.to_a
        expect(find_command.command['$readPreference']).to eq('mode' => 'secondary')
      end

      context 'when defined on the collection' do
        let(:collection_options) do
          { read: { mode: :secondary_preferred } }
        end

        it 'uses the read concern defined on the collection' do
          collection.find.to_a
          expect(find_command.command['$readPreference']).to eq('mode' => 'secondaryPreferred')
        end
      end
    end
  end
end
