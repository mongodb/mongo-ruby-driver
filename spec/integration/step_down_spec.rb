require 'spec_helper'

describe 'Step down behavior' do
  require_topology :replica_set
  min_server_fcv '4.2'

  before(:all) do
    # These before/after blocks are run even if the tests themselves are
    # skipped due to server version not being appropriate
    if ClusterConfig.instance.fcv_ish >= '4.2' && ClusterConfig.instance.topology == :replica_set
      ClusterTools.instance.set_election_timeout(1)
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

  let(:test_client) do
    authorized_client.with(server_selection_timeout: 10)
  end

  describe 'getMore iteration' do

    let(:subscribed_client) do
      test_client.tap do |client|
        client.subscribe(Mongo::Monitoring::COMMAND, EventSubscriber)
      end
    end

    let(:collection) { subscribed_client['step-down'] }

    before do
      collection.insert_many([{test: 1}] * 100)
    end

    let(:view) { collection.find({test: 1}, batch_size: 10) }
    let(:enum) { view.to_enum }

    it 'continues through step down' do

      EventSubscriber.clear_events!

      # get the first item
      item = enum.next
      expect(item['test']).to eq(1)

      find_events = EventSubscriber.started_events.select do |event|
        event.command['find']
      end
      expect(find_events.length).to eq(1)
      find_socket_object_id = find_events.first.socket_object_id
      expect(find_socket_object_id).to be_a(Numeric)

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
      get_more_socket_object_id = get_more_events.first.socket_object_id
      expect(get_more_socket_object_id).to eq(find_socket_object_id)
    end
  end

  describe 'writes on connections' do
    let(:server) do
      client = test_client.with(app_name: rand)
      client['test'].insert_one(test: 1)
      client.cluster.next_primary
    end

    let(:connection) { server.pool.check_out }
    let(:operation) do
      Mongo::Operation::Insert::OpMsg.new(
        documents: [{test: 1}],
        db_name: SpecConfig.instance.test_db,
        coll_name: 'step-down',
        write_concern: Mongo::WriteConcern.get(write_concern),
      )
    end
    let(:first_message) do
      Mongo::Operation::Insert::OpMsg.new(
        documents: [{test: 1}],
        db_name: SpecConfig.instance.test_db,
        coll_name: 'step-down',
        write_concern: Mongo::WriteConcern.get(w: 1),
      ).send(:message, server)
    end

    after do
      server.pool.check_in(connection)
    end

    describe 'acknowledged write after step down' do
      let(:write_concern) { {:w => 1} }

      let(:test_operation) do
        rv = connection.dispatch([first_message], 1)
        expect(rv.documents.first['ok']).to eq(1)

        ClusterTools.instance.change_primary

        message = operation.send(:message, server)
        rv = connection.dispatch([message], 1)
        doc = rv.documents.first
        expect(doc['ok']).to eq(0)
        expect(doc['codeName']).to eq('NotMaster')
      end


      it 'keeps connection open' do
        test_operation

        expect(connection.connected?).to be true
      end
    end

    describe 'unacknowledged write after step down' do
      let(:write_concern) { {:w => 0} }

      it 'closes the connection' do
        rv = connection.dispatch([first_message], 1)
        expect(rv.documents.first['ok']).to eq(1)

        ClusterTools.instance.change_primary

        existing_socket = connection.send(:socket)
        expect(existing_socket).not_to be nil

        message = operation.send(:message, server)
        connection.dispatch([message], 2)
        # No response will be returned hence we have no response assertions here

        expect(connection.send(:socket)).to be(existing_socket)

        expect do
          # Due to buffering in the network stack, the second unacknowledged
          # write may succeed. Send two to guarantee failure
          message = operation.send(:message, server)
          connection.dispatch([message], 3)
          sleep 0.5
          message = operation.send(:message, server)
          connection.dispatch([message], 4)
        end.to raise_error(Mongo::Error::SocketError)
      end
    end

    describe 'acknowledged write over connection to primary-secondary-primary' do

      let(:write_concern) { {:w => 1} }

      before do
        ClusterTools.instance.unfreeze_all
      end

      it 'succeeds' do
        rv = connection.dispatch([first_message], 1)
        expect(rv.documents.first['ok']).to eq(1)

        test_client.cluster.next_primary.unknown!
        current_primary_address = test_client.cluster.next_primary.address
        puts "#{Time.now} Current primary is #{current_primary_address}, changing to something else"
        ClusterTools.instance.change_primary
        primary_changed_at = Time.now
        test_client.cluster.next_primary.unknown!
        puts "#{Time.now} Now primary is #{test_client.cluster.next_primary.address}"

        message = operation.send(:message, server)
        rv = connection.dispatch([message], 1)
        doc = rv.documents.first
        expect(doc['ok']).to eq(0)
        expect(doc['codeName']).to eq('NotMaster')

        expect(connection.connected?).to be true

        test_client.cluster.next_primary.unknown!
        expect(test_client.cluster.next_primary.address).not_to eq(current_primary_address)

        puts "#{Time.now} Asking to make #{current_primary_address} primary again"
        ClusterTools.instance.force_primary(current_primary_address)
        puts "#{Time.now} Now primary is #{test_client.cluster.next_primary.address}"
        test_client.cluster.servers_list.each do |server|
          server.unknown!
        end
        expect(test_client.cluster.next_primary.address).to eq(current_primary_address)

        message = operation.send(:message, server)
        rv = connection.dispatch([message], 1)
        doc = rv.documents.first
        expect(doc['ok']).to eq(1)

        expect(connection.connected?).to be true
      end
    end
  end
end
