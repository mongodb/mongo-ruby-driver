shared_examples 'an operation using a session' do

  describe 'operation execution', if: sessions_enabled? do

    let(:session) do
      authorized_client.start_session do |s|
        expect(s).to receive(:use).and_call_original
      end
    end

    let!(:before_last_use) do
      session.instance_variable_get(:@last_use)
    end

    let!(:before_operation_time) do
      session.instance_variable_get(:@operation_time)
    end

    let!(:result) do
      operation
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