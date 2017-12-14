shared_examples 'an operation using a session' do

  describe 'operation execution', if: test_sessions? do

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
        (session.instance_variable_get(:@operation_time) || 0)
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
        expect(session.instance_variable_get(:@operation_time)).not_to eq(before_operation_time)
      end

      it 'does not close the session when the operation completes' do
        expect(session.ended?).to be(false)
      end
    end

    context 'when a session from another client is provided' do

      let(:session) do
        client.start_session
      end

      let(:client) do
        authorized_client.with(read: { mode: :secondary })
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

  context 'when the operation fails', if: test_sessions? do

    let!(:before_last_use) do
      session.instance_variable_get(:@server_session).last_use
    end

    let!(:before_operation_time) do
      (session.instance_variable_get(:@operation_time) || 0)
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
      expect(session.instance_variable_get(:@operation_time)).not_to eq(before_operation_time)
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
    authorized_client.with(heartbeat_frequency: 100).tap do |cl|
      cl.subscribe(Mongo::Monitoring::COMMAND, subscriber)
    end
  end

  let(:subscriber) do
    EventSubscriber.new
  end

  after do
    client.close
  end

  context 'when the command is run once' do

    context 'when the server is version 3.6' do

      context 'when the cluster is sharded or a replica set', if: (!standalone? && sessions_enabled?) do

        let!(:reply_cluster_time) do
          operation_with_session
          subscriber.succeeded_events[-1].reply['$clusterTime']
        end

        it 'updates the cluster time of the cluster' do
          expect(cluster.cluster_time).to eq(reply_cluster_time)
        end

        it 'updates the cluster time of the session' do
          expect(session.cluster_time).to eq(reply_cluster_time)
        end
      end

      context 'when the server is a standalone', if: (standalone? && sessions_enabled?) do

        let(:before_cluster_time) do
          client.cluster.cluster_time
        end

        let!(:reply_cluster_time) do
          operation_with_session
          subscriber.succeeded_events[-1].reply['$clusterTime']
        end

        it 'does not update the cluster time of the cluster' do
          expect(before_cluster_time).to eq(before_cluster_time)
        end

        it 'does not update the cluster time of the session' do
          expect(session.cluster_time).to be_nil
        end
      end
    end

    context 'when the server is less than version 3.6', if: !sessions_enabled? do

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
      operation_with_session
      subscriber.succeeded_events[-1].reply['$clusterTime']
    end

    context 'when the cluster is sharded or a replica set', if: test_sessions? do

      context 'when the session cluster time is advanced' do

        before do
          session.advance_cluster_time(advanced_cluster_time)
        end

        let(:second_command_cluster_time) do
          second_operation
          subscriber.started_events[-1].command['$clusterTime']
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
          subscriber.started_events[-1].command['$clusterTime']
        end

        it 'includes the received cluster time in the second command' do
          expect(second_command_cluster_time).to eq(reply_cluster_time)
        end
      end
    end

    context 'when the server is a standalone', if: (standalone? && sessions_enabled?) do

      let(:before_cluster_time) do
        client.cluster.cluster_time
      end

      let(:second_command_cluster_time) do
        second_operation
        subscriber.started_events[-1].command['$clusterTime']
      end

      it 'does not update the cluster time of the cluster' do
        second_command_cluster_time
        expect(before_cluster_time).to eq(before_cluster_time)
      end
    end
  end

  context 'when the server is less than version 3.6', if: !sessions_enabled? do

    let(:before_cluster_time) do
      client.cluster.cluster_time
    end

    it 'does not update the cluster time of the cluster' do
      operation
      expect(before_cluster_time).to eq(before_cluster_time)
    end
  end
end
