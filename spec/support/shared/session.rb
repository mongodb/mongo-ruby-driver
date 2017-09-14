shared_examples 'an operation using a session' do

  describe 'operation execution', if: sessions_enabled? do

    context 'when the session is created from the same client used for the operation' do

      let(:session) do
        authorized_client.start_session
      end

      let!(:before_last_use) do
        session.instance_variable_get(:@last_use)
      end

      let!(:before_operation_time) do
        (session.instance_variable_get(:@operation_time) || 0)
      end

      let!(:operation_result) do
        operation
      end

      it 'updates the last use value' do
        expect(session.instance_variable_get(:@last_use)).not_to eq(before_last_use)
      end

      it 'updates the operation time value' do
        expect(session.instance_variable_get(:@operation_time)).not_to eq(before_operation_time)
      end
    end

    context 'when a session from another client is provided' do

      let(:session) do
        client.start_session
      end

      let(:client) do
        authorized_client.with(heartbeat_frequency: 10)
      end

      after do
        client.close
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
        authorized_client.start_session
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

  let(:session) do
    authorized_client.start_session
  end

  let!(:before_last_use) do
    session.instance_variable_get(:@last_use)
  end

  let!(:before_operation_time) do
    (session.instance_variable_get(:@operation_time) || 0)
  end

  let!(:operation_result) do
    operation
  end

  let(:operation_result) do
    begin; failed_operation; rescue => e; e; end
  end

  context 'when the operation fails', if: sessions_enabled? do

    it 'raises an error' do
      expect([Mongo::Error::OperationFailure,
              Mongo::Error::BulkWriteError]).to include(operation_result.class)
    end

    it 'updates the last use value' do
      expect(session.instance_variable_get(:@last_use)).not_to eq(before_last_use)
    end

    it 'updates the operation time value' do
      expect(session.instance_variable_get(:@operation_time)).not_to eq(before_operation_time)
    end
  end
end

shared_examples 'an operation updating cluster time' do

  let(:cluster) do
    client.cluster
  end

  let(:client) do
    Mongo::Client.new(ADDRESSES, TEST_OPTIONS.merge(heartbeat_frequency: 100)).tap do |cl|
      cl.subscribe(Mongo::Monitoring::COMMAND, subscriber)
    end
  end

  let(:subscriber) do
    EventSubscriber.new
  end

  context 'when the command is run once' do

    context 'when the server is version 3.6' do

      context 'when the server is a mongos', if: (sharded? && op_msg_enabled?) do

        let!(:reply_cluster_time) do
          operation
          subscriber.succeeded_events[-1].reply['$clusterTime']
        end

        it 'updates the cluster time of the cluster' do
          expect(cluster.cluster_time).to eq(reply_cluster_time)
        end
      end

      context 'when the server is not a mongos', if: (!sharded? && op_msg_enabled?) do

        let(:before_cluster_time) do
          client.cluster.cluster_time
        end

        let!(:reply_cluster_time) do
          operation
          subscriber.succeeded_events[-1].reply['$clusterTime']
        end

        it 'does not update the cluster time of the cluster' do
          expect(before_cluster_time).to eq(before_cluster_time)
        end
      end
    end

    context 'when the server is less than version 3.6', if: !op_msg_enabled? do

      let(:before_cluster_time) do
        client.cluster.cluster_time
      end

      let!(:reply_cluster_time) do
        operation
        subscriber.succeeded_events[-1].reply['$clusterTime']
      end

      it 'does not update the cluster time of the cluster' do
        expect(before_cluster_time).to eq(before_cluster_time)
      end
    end
  end

  context 'when the command is run twice' do

    let!(:reply_cluster_time) do
      operation
      subscriber.succeeded_events[-1].reply['$clusterTime']
    end

    let(:second_command_cluster_time) do
      second_operation
      subscriber.started_events[-1].command['$clusterTime']
    end

    context 'when the server is a mongos', if: (sharded? && op_msg_enabled?) do

      it 'includes the received cluster time in the second command' do
        expect(second_command_cluster_time).to eq(reply_cluster_time)
      end
    end

    context 'when the server is not a mongos', if: (!sharded? && op_msg_enabled?) do

      let(:before_cluster_time) do
        client.cluster.cluster_time
      end

      it 'does not update the cluster time of the cluster' do
        second_command_cluster_time
        expect(before_cluster_time).to eq(before_cluster_time)
      end
    end
  end

  context 'when the server is less than version 3.6', if: !op_msg_enabled? do

    let(:before_cluster_time) do
      client.cluster.cluster_time
    end

    it 'does not update the cluster time of the cluster' do
      operation
      expect(before_cluster_time).to eq(before_cluster_time)
    end
  end
end