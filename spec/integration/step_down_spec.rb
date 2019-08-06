require 'spec_helper'

describe 'Step down behavior' do
  require_topology :replica_set

  # This setup reduces the runtime of the test and makes execution more
  # reliable. The spec as written requests a simple brute force step down,
  # but this causes intermittent failures.
  before(:all) do
    # These before/after blocks are run even if the tests themselves are
    # skipped due to server version not being appropriate
    if ClusterConfig.instance.fcv_ish >= '4.2' && ClusterConfig.instance.topology == :replica_set
      # It seems that a short election timeout can cause unintended elections,
      # which makes the server close connections which causes the driver to
      # reconnect which then fails the step down test.
      # The election timeout here is greater than the catch up period and
      # step down timeout specified in cluster tools.
      ClusterTools.instance.set_election_timeout(5)
      ClusterTools.instance.set_election_handoff(false)
    end
  end

  after(:all) do
    if ClusterConfig.instance.fcv_ish >= '4.2' && ClusterConfig.instance.topology == :replica_set
      ClusterTools.instance.set_election_timeout(10)
      ClusterTools.instance.set_election_handoff(true)
      ClusterTools.instance.reset_priorities
    end
  end

  let(:event_subscriber) { EventSubscriber.new }

  let(:test_client) do
    authorized_client_without_any_retries.with(server_selection_timeout: 20).tap do |client|
      client.subscribe(Mongo::Monitoring::CONNECTION_POOL, event_subscriber)
    end
  end

  let(:collection) { test_client['step-down'].with(write: write_concern) }

  let(:admin_support_client) do
    ClientRegistry.instance.global_client('root_authorized').use('admin')
  end

  describe 'getMore iteration' do
    min_server_fcv '4.2'

    let(:subscribed_client) do
      test_client.tap do |client|
        client.subscribe(Mongo::Monitoring::COMMAND, EventSubscriber)
        client.subscribe(Mongo::Monitoring::CONNECTION_POOL, EventSubscriber)
      end
    end

    let(:collection) { subscribed_client['step-down'] }

    before do
      collection.insert_many([{test: 1}] * 100)
    end

    let(:view) { collection.find({test: 1}, batch_size: 10) }
    let(:enum) { view.to_enum }

    it 'continues through step down' do

      subscribed_client.cluster.next_primary.pool.clear
      event_subscriber.clear_events!

      # get the first item
      item = enum.next
      expect(item['test']).to eq(1)

      connection_created_events = EventSubscriber.published_events.select do |event|
        event.is_a?(Mongo::Monitoring::Event::Cmap::ConnectionCreated)
      end
      expect(connection_created_events).not_to be_empty

      current_primary = subscribed_client.cluster.next_primary
      ClusterTools.instance.change_primary

      EventSubscriber.clear_events!

      # exhaust the batch
      9.times do
        enum.next
      end

      # this should issue a getMore
      item = enum.next
      expect(item['test']).to eq(1)

      get_more_events = EventSubscriber.started_events.select do |event|
        event.command['getMore']
      end

      expect(get_more_events.length).to eq(1)

      # getMore should have been sent on the same connection as find
      connection_created_events = EventSubscriber.published_events.select do |event|
        event.is_a?(Mongo::Monitoring::Event::Cmap::ConnectionCreated)
      end
      expect(connection_created_events).to be_empty
    end

    after do
      # The tests normally operate with a low server selection timeout,
      # but since this test caused a cluster election we may need to wait
      # longer for the cluster to reestablish itself.
      # To keep all other tests' timeouts low, wait for primary to be
      # elected at the end of this test
      test_client.cluster.servers.each do |server|
        server.unknown!
      end
      test_client.cluster.next_primary

      # Since we are changing which server is primary, close all clients
      # to prevent subsequent tests setting fail points on servers which
      # are not primary
      ClientRegistry.instance.close_all_clients
    end
  end

  describe 'writes on connections' do
    min_server_fcv '4.0'

    let(:server) do
      client = test_client.with(app_name: rand)
      client['test'].insert_one(test: 1)
      client.cluster.next_primary
    end

    let(:fail_point) do
      { configureFailPoint: 'failCommand', data: {
        # There is currently no way to turn write retries on not master
        # errors off - therefore set the number of fails to 2
        failCommands: ['insert'], errorCode: fail_point_code, }, mode: {times: 2} }
    end

    before do
      collection.find
      admin_support_client.command(fail_point)
    end

    after do
      admin_support_client.command(configureFailPoint: 'failCommand', mode: 'off')
    end

    describe 'not master - 4.2' do
      min_server_fcv '4.2'

      let(:write_concern) { {:w => 1} }

      # not master
      let(:fail_point_code) { 10107 }

      it 'keeps connection open' do
        event_subscriber.clear_events!

        expect do
          collection.insert_one(test: 1)
        end.to raise_error(Mongo::Error::OperationFailure, /10107/)

        # Temporarily add 1 due to RUBY-1894 backport
        expect(event_subscriber.select_published_events(Mongo::Monitoring::Event::Cmap::PoolCleared).count).to eq(0+1)
      end
    end

    describe 'not master - 4.0' do
      max_server_version '4.0'

      let(:write_concern) { {:w => 1} }

      # not master
      let(:fail_point_code) { 10107 }

      it 'closes the connection' do
        event_subscriber.clear_events!

        expect do
          collection.insert_one(test: 1)
        end.to raise_error(Mongo::Error::OperationFailure, /10107/)

        # Temporarily add 1 due to RUBY-1894 backport
        expect(event_subscriber.select_published_events(Mongo::Monitoring::Event::Cmap::PoolCleared).count).to eq(1+1)
      end
    end

    describe 'node shutting down' do
      let(:write_concern) { {:w => 1} }

      # interrupted at shutdown
      let(:fail_point_code) { 11600 }

      it 'closes the connection' do
        event_subscriber.clear_events!

        expect do
          collection.insert_one(test: 1)
        end.to raise_error(Mongo::Error::OperationFailure, /11600/)

        # Temporarily add 1 due to RUBY-1894 backport
        expect(event_subscriber.select_published_events(Mongo::Monitoring::Event::Cmap::PoolCleared).count).to eq(1+1)
      end
    end
  end
end
