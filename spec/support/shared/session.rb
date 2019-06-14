shared_examples 'an operation using a session' do

  describe 'operation execution' do
    min_server_fcv '3.6'
    require_topology :replica_set, :sharded

    context 'when the session is created from the same client used for the operation' do

      let(:session) do
        client.start_session
      end

      let(:server_session) do
        session.instance_variable_get(:@server_session)
      end

      let!(:before_last_use) do
        server_session.last_use
      end

      let!(:before_operation_time) do
        (session.operation_time || 0)
      end

      let!(:operation_result) do
        operation
      end

      after do
        session.end_session
      end

      it 'updates the last use value' do
        expect(server_session.last_use).not_to eq(before_last_use)
      end

      it 'updates the operation time value' do
        expect(session.operation_time).not_to eq(before_operation_time)
      end

      it 'does not close the session when the operation completes' do
        expect(session.ended?).to be(false)
      end
    end

    context 'when a session from another client is provided' do

      let(:session) do
        another_authorized_client.start_session
      end

      let(:operation_result) do
        operation
      end

      it 'raises an exception' do
        expect {
          operation_result
        }.to raise_exception(Mongo::Error::InvalidSession)
      end
    end

    context 'when the session is ended before it is used' do

      let(:session) do
        client.start_session
      end

      before do
        session.end_session
      end

      let(:operation_result) do
        operation
      end

      it 'raises an exception' do
        expect {
          operation_result
        }.to raise_exception(Mongo::Error::InvalidSession)
      end
    end
  end
end

shared_examples 'a failed operation using a session' do

  context 'when the operation fails' do
    min_server_fcv '3.6'
    require_topology :replica_set, :sharded

    let!(:before_last_use) do
      session.instance_variable_get(:@server_session).last_use
    end

    let!(:before_operation_time) do
      (session.operation_time || 0)
    end

    let!(:operation_result) do
      sleep 0.2
      begin; failed_operation; rescue => e; e; end
    end

    let(:session) do
      client.start_session
    end

    it 'raises an error' do
      expect([Mongo::Error::OperationFailure,
              Mongo::Error::BulkWriteError]).to include(operation_result.class)
    end

    it 'updates the last use value' do
      expect(session.instance_variable_get(:@server_session).last_use).not_to eq(before_last_use)
    end

    it 'updates the operation time value' do
      expect(session.operation_time).not_to eq(before_operation_time)
    end
  end
end

shared_examples 'an explicit session with an unacknowledged write' do

  before do
    EventSubscriber.clear_events!
  end

  context 'when sessions are supported' do
    min_server_fcv '3.6'

    let(:session) do
      client.start_session
    end

    it 'does not add a session id to the operation' do
      operation
      expect(EventSubscriber.started_events.collect(&:command).collect { |cmd| cmd['lsid'] }.compact).to be_empty
    end
  end

  context 'when sessions are not supported' do
    max_server_version '3.4'

    let(:session) do
      double('session').tap do |s|
        allow(s).to receive(:validate!)
      end
    end

    it 'does not add a session id to the operation' do
      operation
      expect(EventSubscriber.started_events.collect(&:command).collect { |cmd| cmd['lsid'] }.compact).to be_empty
    end
  end
end

shared_examples 'an implicit session with an unacknowledged write' do

  before do
    EventSubscriber.clear_events!
  end

  context 'when sessions are supported' do
    min_server_fcv '3.6'

    it 'does not add a session id to the operation' do
      operation
      expect(EventSubscriber.started_events.collect(&:command).collect { |cmd| cmd['lsid'] }.compact).to be_empty
    end
  end

  context 'when sessions are not supported' do
    max_server_version '3.4'

    it 'does not add a session id to the operation' do
      operation
      expect(EventSubscriber.started_events.collect(&:command).collect { |cmd| cmd['lsid'] }.compact).to be_empty
    end
  end
end

shared_examples 'an operation supporting causally consistent reads' do

  let(:client) do
    subscribed_client
  end

  context 'when connected to a standalone' do
    min_server_fcv '3.6'
    require_topology :single

    context 'when the collection specifies a read concern' do

      let(:collection) do
        client[TEST_COLL, read_concern: { level: 'majority' }]
      end

      context 'when the session has causal_consistency set to true' do

        let(:session) do
          client.start_session(causal_consistency: true)
        end

        it 'does not add the afterClusterTime to the read concern in the command' do
          expect(command['readConcern']['afterClusterTime']).to be_nil
        end
      end

      context 'when the session has causal_consistency set to false' do

        let(:session) do
          client.start_session(causal_consistency: false)
        end

        it 'does not add the afterClusterTime to the read concern in the command' do
          expect(command['readConcern']['afterClusterTime']).to be_nil
        end
      end

      context 'when the session has causal_consistency not set' do

        let(:session) do
          client.start_session
        end

        it 'does not add the afterClusterTime to the read concern in the command' do
          expect(command['readConcern']['afterClusterTime']).to be_nil
        end
      end
    end

    context 'when the collection does not specify a read concern' do

      let(:collection) do
        client[TEST_COLL]
      end

      context 'when the session has causal_consistency set to true' do

        let(:session) do
          client.start_session(causal_consistency: true)
        end

        it 'does not include the read concern in the command' do
          expect(command['readConcern']).to be_nil
        end
      end

      context 'when the session has causal_consistency set to false' do

        let(:session) do
          client.start_session(causal_consistency: false)
        end

        it 'does not include the read concern in the command' do
          expect(command['readConcern']).to be_nil
        end
      end

      context 'when the session has causal_consistency not set' do

        let(:session) do
          client.start_session
        end

        it 'does not include the read concern in the command' do
          expect(command['readConcern']).to be_nil
        end
      end
    end
  end

  context 'when connected to replica set or sharded cluster' do
    min_server_fcv '3.6'
    require_topology :replica_set, :sharded

    context 'when the collection specifies a read concern' do

      let(:collection) do
        client[TEST_COLL, read_concern: { level: 'majority' }]
      end

      context 'when the session has causal_consistency set to true' do

        let(:session) do
          client.start_session(causal_consistency: true)
        end

        context 'when the session has an operation time' do

          before do
            client.database.command({ ping: 1 }, session: session)
          end

          let!(:operation_time) do
            session.operation_time
          end

          let(:expected_read_concern) do
            BSON::Document.new(level: 'majority', afterClusterTime: operation_time)
          end

          it 'merges the afterClusterTime with the read concern in the command' do
            expect(command['readConcern']).to eq(expected_read_concern)
          end
        end

        context 'when the session does not have an operation time' do

          let(:expected_read_concern) do
            BSON::Document.new(level: 'majority')
          end

          it 'leaves the read concern document unchanged' do
            expect(command['readConcern']).to eq(expected_read_concern)
          end
        end

        context 'when the operation time is advanced' do

          before do
            session.advance_operation_time(operation_time)
          end

          let(:operation_time) do
            BSON::Timestamp.new(0, 1)
          end

          let(:expected_read_concern) do
            BSON::Document.new(level: 'majority', afterClusterTime: operation_time)
          end

          it 'merges the afterClusterTime with the new operation time and read concern in the command' do
            expect(command['readConcern']).to eq(expected_read_concern)
          end
        end
      end

      context 'when the session has causal_consistency set to false' do

        let(:session) do
          client.start_session(causal_consistency: false)
        end

        context 'when the session does not have an operation time' do

          let(:expected_read_concern) do
            BSON::Document.new(level: 'majority')
          end

          it 'leaves the read concern document unchanged' do
            expect(command['readConcern']).to eq(expected_read_concern)
          end
        end

        context 'when the session has an operation time' do

          before do
            client.database.command({ ping: 1 }, session: session)
          end

          let(:expected_read_concern) do
            BSON::Document.new(level: 'majority')
          end

          it 'leaves the read concern document unchanged' do
            expect(command['readConcern']).to eq(expected_read_concern)
          end
        end

        context 'when the operation time is advanced' do

          before do
            session.advance_operation_time(operation_time)
          end

          let(:operation_time) do
            BSON::Timestamp.new(0, 1)
          end

          let(:expected_read_concern) do
            BSON::Document.new(level: 'majority')
          end

          it 'leaves the read concern document unchanged' do
            expect(command['readConcern']).to eq(expected_read_concern)
          end
        end
      end

      context 'when the session has causal_consistency not set' do

        let(:session) do
          client.start_session
        end

        context 'when the session does not have an operation time' do

          let(:expected_read_concern) do
            BSON::Document.new(level: 'majority')
          end

          it 'leaves the read concern document unchanged' do
            expect(command['readConcern']).to eq(expected_read_concern)
          end
        end

        context 'when the session has an operation time' do

          before do
            client.database.command({ ping: 1 }, session: session)
          end

          let!(:operation_time) do
            session.operation_time
          end

          let(:expected_read_concern) do
            BSON::Document.new(level: 'majority', afterClusterTime: operation_time)
          end

          it 'merges the afterClusterTime with the new operation time and read concern in the command' do
            expect(command['readConcern']).to eq(expected_read_concern)
          end
        end

        context 'when the operation time is advanced' do

          before do
            session.advance_operation_time(operation_time)
          end

          let(:operation_time) do
            BSON::Timestamp.new(0, 1)
          end

          let(:expected_read_concern) do
            BSON::Document.new(level: 'majority', afterClusterTime: operation_time)
          end

          it 'merges the afterClusterTime with the new operation time and read concern in the command' do
            expect(command['readConcern']).to eq(expected_read_concern)
          end
        end
      end
    end

    context 'when the collection does not specify a read concern' do

      let(:collection) do
        client[TEST_COLL]
      end

      context 'when the session has causal_consistency set to true' do

        let(:session) do
          client.start_session(causal_consistency: true)
        end

        context 'when the session does not have an operation time' do

          it 'does not include the read concern in the command' do
            expect(command['readConcern']).to be_nil
          end
        end

        context 'when the session has an operation time' do

          before do
            client.database.command({ ping: 1 }, session: session)
          end

          let!(:operation_time) do
            session.operation_time
          end

          let(:expected_read_concern) do
            BSON::Document.new(afterClusterTime: operation_time)
          end

          it 'merges the afterClusterTime with the read concern in the command' do
            expect(command['readConcern']).to eq(expected_read_concern)
          end
        end

        context 'when the operation time is advanced' do

          before do
            session.advance_operation_time(operation_time)
          end

          let(:operation_time) do
            BSON::Timestamp.new(0, 1)
          end

          let(:expected_read_concern) do
            BSON::Document.new(afterClusterTime: operation_time)
          end

          it 'merges the afterClusterTime with the new operation time in the command' do
            expect(command['readConcern']).to eq(expected_read_concern)
          end
        end
      end

      context 'when the session has causal_consistency set to false' do

        let(:session) do
          client.start_session(causal_consistency: false)
        end

        context 'when the session does not have an operation time' do

          it 'does not include the read concern in the command' do
            expect(command['readConcern']).to be_nil
          end
        end

        context 'when the session has an operation time' do

          before do
            client.database.command({ ping: 1 }, session: session)
          end

          it 'does not include the read concern in the command' do
            expect(command['readConcern']).to be_nil
          end
        end

        context 'when the operation time is advanced' do

          before do
            session.advance_operation_time(operation_time)
          end

          let(:operation_time) do
            BSON::Timestamp.new(0, 1)
          end

          let(:expected_read_concern) do
            BSON::Document.new(afterClusterTime: operation_time)
          end

          it 'does not include the read concern in the command' do
            expect(command['readConcern']).to be_nil
          end
        end
      end

      context 'when the session has causal_consistency not set' do

        let(:session) do
          client.start_session
        end

        context 'when the session does not have an operation time' do

          it 'does not include the read concern in the command' do
            expect(command['readConcern']).to be_nil
          end
        end

        context 'when the session has an operation time' do

          before do
            client.database.command({ ping: 1 }, session: session)
          end

          let!(:operation_time) do
            session.operation_time
          end

          let(:expected_read_concern) do
            BSON::Document.new(afterClusterTime: operation_time)
          end

          it 'merges the afterClusterTime with the read concern in the command' do
            expect(command['readConcern']).to eq(expected_read_concern)
          end
        end

        context 'when the operation time is advanced' do

          before do
            session.advance_operation_time(operation_time)
          end

          let(:operation_time) do
            BSON::Timestamp.new(0, 1)
          end

          let(:expected_read_concern) do
            BSON::Document.new(afterClusterTime: operation_time)
          end

          it 'merges the afterClusterTime with the new operation time in the command' do
            expect(command['readConcern']).to eq(expected_read_concern)
          end
        end
      end
    end
  end
end

shared_examples 'an operation updating cluster time' do

  let(:cluster) do
    client.cluster
  end

  let(:session) do
    client.start_session
  end

  let(:client) do
    subscribed_client
  end

  shared_examples_for 'does not update the cluster time of the cluster' do
    it 'does not update the cluster time of the cluster' do
      bct = before_cluster_time
      reply_cluster_time
      expect(client.cluster.cluster_time).to eq(before_cluster_time)
    end
  end

  context 'when the command is run once' do

    context 'when the server is version 3.6' do
      min_server_fcv '3.6'

      context 'when the cluster is sharded or a replica set' do
        require_topology :replica_set, :sharded

        let(:reply_cluster_time) do
          operation_with_session
          EventSubscriber.succeeded_events[-1].reply['$clusterTime']
        end

        it 'updates the cluster time of the cluster' do
          rct = reply_cluster_time
          expect(cluster.cluster_time).to eq(rct)
        end

        it 'updates the cluster time of the session' do
          rct = reply_cluster_time
          expect(session.cluster_time).to eq(rct)
        end
      end

      context 'when the server is a standalone' do
        require_topology :single

        let(:before_cluster_time) do
          client.cluster.cluster_time
        end

        let!(:reply_cluster_time) do
          operation_with_session
          EventSubscriber.succeeded_events[-1].reply['$clusterTime']
        end

        it_behaves_like 'does not update the cluster time of the cluster'

        it 'does not update the cluster time of the session' do
          reply_cluster_time
          expect(session.cluster_time).to be_nil
        end
      end
    end

    context 'when the server is less than version 3.6' do
      max_server_version '3.4'

      let(:before_cluster_time) do
        client.cluster.cluster_time
      end

      let(:reply_cluster_time) do
        operation
        EventSubscriber.succeeded_events[-1].reply['$clusterTime']
      end

      it_behaves_like 'does not update the cluster time of the cluster'
    end
  end

  context 'when the command is run twice' do

    let(:reply_cluster_time) do
      operation_with_session
      EventSubscriber.succeeded_events[-1].reply['$clusterTime']
    end

    context 'when the cluster is sharded or a replica set' do
      min_server_fcv '3.6'
      require_topology :replica_set, :sharded

      context 'when the session cluster time is advanced' do

        before do
          session.advance_cluster_time(advanced_cluster_time)
        end

        let(:second_command_cluster_time) do
          second_operation
          EventSubscriber.started_events[-1].command['$clusterTime']
        end

        context 'when the advanced cluster time is greater than the existing cluster time' do

          let(:advanced_cluster_time) do
            new_timestamp = BSON::Timestamp.new(reply_cluster_time[Mongo::Cluster::CLUSTER_TIME].seconds,
                                                reply_cluster_time[Mongo::Cluster::CLUSTER_TIME].increment + 1)
            new_cluster_time = reply_cluster_time.dup
            new_cluster_time.merge(Mongo::Cluster::CLUSTER_TIME => new_timestamp)
          end

          it 'includes the advanced cluster time in the second command' do
            expect(second_command_cluster_time).to eq(advanced_cluster_time)
          end
        end

        context 'when the advanced cluster time is not greater than the existing cluster time' do

          let(:advanced_cluster_time) do
            expect(reply_cluster_time[Mongo::Cluster::CLUSTER_TIME].increment > 0).to be true

            new_timestamp = BSON::Timestamp.new(reply_cluster_time[Mongo::Cluster::CLUSTER_TIME].seconds,
                                                reply_cluster_time[Mongo::Cluster::CLUSTER_TIME].increment - 1)
            new_cluster_time = reply_cluster_time.dup
            new_cluster_time.merge(Mongo::Cluster::CLUSTER_TIME => new_timestamp)
          end

          it 'does not advance the cluster time' do
            expect(second_command_cluster_time).to eq(reply_cluster_time)
          end
        end
      end

      context 'when the session cluster time is not advanced' do

        let(:second_command_cluster_time) do
          second_operation
          EventSubscriber.started_events[-1].command['$clusterTime']
        end

        it 'includes the received cluster time in the second command' do
          reply_cluster_time
          expect(second_command_cluster_time).to eq(reply_cluster_time)
        end
      end
    end

    context 'when the server is a standalone' do
      min_server_fcv '3.6'
      require_topology :single

      let(:before_cluster_time) do
        client.cluster.cluster_time
      end

      let(:second_command_cluster_time) do
        second_operation
        EventSubscriber.started_events[-1].command['$clusterTime']
      end

      it 'does not update the cluster time of the cluster' do
        bct = before_cluster_time
        second_command_cluster_time
        expect(client.cluster.cluster_time).to eq(bct)
      end
    end
  end

  context 'when the server is less than version 3.6' do
    max_server_version '3.4'

    let(:before_cluster_time) do
      client.cluster.cluster_time
    end

    it 'does not update the cluster time of the cluster' do
      bct = before_cluster_time
      operation
      expect(client.cluster.cluster_time).to eq(bct)
    end
  end
end

shared_examples 'an operation not using a session' do
  min_server_fcv '3.6'

  describe 'operation execution' do

    context 'when the client has a session' do

      let(:session) do
        client.start_session
      end

      let(:server_session) do
        session.instance_variable_get(:@server_session)
      end

      let!(:before_last_use) do
        server_session.last_use
      end

      let!(:before_operation_time) do
        session.operation_time
      end

      let!(:operation_result) do
        operation
      end

      after do
        session.end_session
      end

      it 'does not send session id in command' do
        expect(command).not_to have_key('lsid')
      end

      it 'does not update the last use value' do
        expect(server_session.last_use).to eq(before_last_use)
      end

      it 'does not update the operation time value' do
        expect(session.operation_time).to eq(before_operation_time)
      end

      it 'does not close the session when the operation completes' do
        expect(session.ended?).to be(false)
      end
    end

    context 'when the session is ended before it is used' do
      let(:session) do
        client.start_session
      end

      before do
        session.end_session
      end

      let(:operation_result) do
        operation
      end

      it 'does not raise an exception' do
        expect {
          operation_result
        }.not_to raise_exception
      end
    end
  end
end

shared_examples 'a failed operation not using a session' do
  min_server_fcv '3.6'

  context 'when the operation fails' do

    let!(:before_last_use) do
      session.instance_variable_get(:@server_session).last_use
    end

    let!(:before_operation_time) do
      session.operation_time
    end

    let!(:operation_result) do
      sleep 0.2
      begin; failed_operation; rescue => e; e; end
    end

    let(:session) do
      client.start_session
    end

    it 'raises an error' do
      expect([Mongo::Error::OperationFailure,
              Mongo::Error::BulkWriteError]).to include(operation_result.class)
    end

    it 'does not update the last use value' do
      expect(session.instance_variable_get(:@server_session).last_use).to eq(before_last_use)
    end

    it 'does not update the operation time value' do
      expect(session.operation_time).to eq(before_operation_time)
    end
  end
end
