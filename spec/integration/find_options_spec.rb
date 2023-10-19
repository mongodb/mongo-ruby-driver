# frozen_string_literal: true

require 'spec_helper'

describe 'Find operation options' do
  require_mri
  require_no_auth
  min_server_fcv '4.4'

  let(:subscriber) { Mrss::EventSubscriber.new }

  let(:seeds) do
    [ SpecConfig.instance.addresses.first ]
  end

  let(:client_options) do
    {}
  end

  let(:collection_options) do
    {}
  end

  let(:client) do
    ClientRegistry.instance.new_local_client(
      seeds,
      SpecConfig.instance.test_options
        .merge(database: SpecConfig.instance.test_db)
        .merge(client_options)
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

  let(:should_create_collection) { true }

  before do
    client['find_options'].drop
    collection.create if should_create_collection
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

      let(:should_create_collection) { false }

      it 'uses the collation defined on the collection' do
        collection.find({}, collation: collation).to_a
        expect(find_command.command['collation']).to eq(collation)
      end
    end
  end

  describe 'read concern' do
    context 'when defined on the client' do
      let(:client_options) do
        { read_concern: { level: :local } }
      end

      let(:collection_options) do
        {}
      end

      it 'uses the read concern defined on the client' do
        collection.find.to_a
        expect(find_command.command['readConcern']).to eq('level' => 'local')
      end

      context 'when defined on the collection' do
        let(:collection_options) do
          { read_concern: { level: :majority } }
        end

        it 'uses the read concern defined on the collection' do
          collection.find.to_a
          expect(find_command.command['readConcern']).to eq('level' => 'majority')
        end

        context 'when defined on the operation' do
          let(:operation_read_concern) do
            { level: :available }
          end

          it 'uses the read concern defined on the operation' do
            collection.find({}, read_concern: operation_read_concern).to_a
            expect(find_command.command['readConcern']).to eq('level' => 'available')
          end
        end
      end

      context 'when defined on the operation' do
        let(:collection_options) do
          {}
        end

        let(:operation_read_concern) do
          { level: :available }
        end

        it 'uses the read concern defined on the operation' do
          collection.find({}, read_concern: operation_read_concern).to_a
          expect(find_command.command['readConcern']).to eq('level' => 'available')
        end
      end
    end

    context 'when defined on the collection' do
      let(:client_options) do
        {}
      end

      let(:collection_options) do
        { read_concern: { level: :majority } }
      end

      it 'uses the read concern defined on the collection' do
        collection.find.to_a
        expect(find_command.command['readConcern']).to eq('level' => 'majority')
      end

      context 'when defined on the operation' do
        let(:operation_read_concern) do
          { level: :available }
        end

        it 'uses the read concern defined on the operation' do
          collection.find({}, read_concern: operation_read_concern).to_a
          expect(find_command.command['readConcern']).to eq('level' => 'available')
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

  describe 'cursor type' do
    let(:collection_options) do
      { capped: true, size: 1000 }
    end

    context 'when cursor type is :tailable' do
      it 'sets the cursor type to tailable' do
        collection.find({}, cursor_type: :tailable).first
        expect(find_command.command['tailable']).to be true
        expect(find_command.command['awaitData']).to be_falsey
      end
    end

    context 'when cursor type is :tailable_await' do
      it 'sets the cursor type to tailable' do
        collection.find({}, cursor_type: :tailable_await).first
        expect(find_command.command['tailable']).to be true
        expect(find_command.command['awaitData']).to be true
      end
    end
  end
end
