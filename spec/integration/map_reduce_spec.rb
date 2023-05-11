# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'Map-reduce operations' do
  let(:client) { authorized_client }
  let(:collection) { client['mr_integration'] }

  let(:subscriber) { Mrss::EventSubscriber.new }

  let(:find_options) { {} }

  let(:operation) do
    collection.find({}, find_options).map_reduce('function(){}', 'function(){}')
  end

  before do
    collection.insert_one(test: 1)

    # Ensure all mongoses are aware of the collection.
    maybe_run_mongos_distincts(collection.database.name, collection.name)

    client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
  end

  let(:event) { subscriber.single_command_started_event('mapReduce') }

  context 'read preference' do
    require_topology :sharded

    context 'specified on client' do
      let(:client) { authorized_client.with(read: {mode: :secondary_preferred }) }

      # RUBY-2706: read preference is not sent on pre-3.6 servers
      min_server_fcv '3.6'

      it 'is sent' do
        operation.to_a

        event.command['$readPreference'].should == {'mode' => 'secondaryPreferred'}
      end
    end

    context 'specified on collection' do
      let(:collection) { client['mr_integration', read: {mode: :secondary_preferred }] }

      # RUBY-2706: read preference is not sent on pre-3.6 servers
      min_server_fcv '3.6'

      it 'is sent' do
        operation.to_a

        event.command['$readPreference'].should == {'mode' => 'secondaryPreferred'}
      end
    end

    context 'specified on operation' do
      let(:find_options) { {read: {mode: :secondary_preferred }} }

      # RUBY-2706: read preference is not sent on pre-3.6 servers
      min_server_fcv '3.6'

      it 'is sent' do
        operation.to_a

        event.command['$readPreference'].should == {'mode' => 'secondaryPreferred'}
      end
    end
  end

  context 'session' do
    min_server_fcv '3.6'

    it 'is sent' do
      operation.to_a

      event.command['lsid'].should_not be nil
    end
  end
end
