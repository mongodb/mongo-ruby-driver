require 'spec_helper'

describe 'Failing retryable operations' do
  # Requirement for fail point
  min_server_fcv '4.0'

  let(:client) do
    subscribed_client
  end

  let(:collection) do
    client['retryable-errors-spec']
  end

  context 'when operation fails' do
    require_topology :replica_set


    let(:clear_fail_point_command) do
      {
        configureFailPoint: 'failCommand',
        mode: 'off',
      }
    end

    after do
      ClusterTools.instance.direct_client_for_each_server do |client|
        client.use(:admin).database.command(clear_fail_point_command)
      end
    end

    let(:collection) do
      client['retryable-errors-spec', read: {mode: :secondary_preferred}]
    end

    let(:first_server) do
      client.cluster.servers_list.detect do |server|
        server.address.seed == events.first.address.seed
      end
    end

    let(:second_server) do
      client.cluster.servers_list.detect do |server|
        server.address.seed == events.last.address.seed
      end
    end

    shared_context 'read operation' do
      let(:fail_point_command) do
        {
          configureFailPoint: 'failCommand',
          mode: {times: 1},
          data: {
            failCommands: ['find'],
            errorCode: 11600,
          },
        }
      end

      let(:set_fail_point) do
        client.cluster.servers_list.each do |server|
          server.monitor.stop!
        end

        ClusterTools.instance.direct_client_for_each_server do |client|
          client.use(:admin).database.command(fail_point_command)
        end
      end

      let(:operation_exception) do
        set_fail_point

        begin
          collection.find(a: 1).to_a
        rescue Mongo::Error::OperationFailure => exception
        else
          fail('Expected operation to fail')
        end

        puts exception.message

        exception
      end

      let(:events) do
        EventSubscriber.command_started_events('find')
      end

      it 'sends reads to different servers' do
      end
    end

    shared_context 'write operation' do
      let(:fail_point_command) do
        {
          configureFailPoint: 'failCommand',
          mode: {times: 2},
          data: {
            failCommands: ['insert'],
            errorCode: 11600,
          },
        }
      end

      let(:set_fail_point) do
        client.use(:admin).database.command(fail_point_command)
      end

      let(:operation_exception) do
        set_fail_point

        begin
          collection.insert_one(a: 1)
        rescue Mongo::Error::OperationFailure => exception
        else
          fail('Expected operation to fail')
        end

        puts exception.message

        exception
      end

      let(:events) do
        EventSubscriber.command_started_events('insert')
      end
    end

    shared_examples_for 'failing retry' do

      it 'indicates second attempt' do
        expect(operation_exception.message).to include('attempt 2')
        expect(operation_exception.message).not_to include('attempt 1')
        expect(operation_exception.message).not_to include('attempt 3')
      end

      it 'publishes two events' do

        expect(events.length).to eq(2)
      end
    end

    shared_examples_for 'failing retry on the same server' do
      it 'is reported on the server of the second attempt' do
        expect(operation_exception.message).to include(second_server.address.seed)
      end
    end

    shared_examples_for 'failing retry on a different server' do
      it 'is reported on the server of the second attempt' do
        expect(operation_exception.message).not_to include(first_server.address.seed)
        expect(operation_exception.message).to include(second_server.address.seed)
      end

      it 'marks servers used in both attempts unknown' do
        operation_exception

        expect(first_server).to be_unknown

        expect(second_server).to be_unknown
      end

      it 'publishes events for the different server addresses' do

        expect(events.length).to eq(2)
        expect(events.first.address.seed).not_to eq(events.last.address.seed)
      end
    end

    shared_examples_for 'modern retry' do
      it 'indicates modern retry' do
        expect(operation_exception.message).to include('modern retry')
        expect(operation_exception.message).not_to include('legacy retry')
      end
    end

    shared_examples_for 'legacy retry' do
      it 'indicates legacy retry' do
        expect(operation_exception.message).to include('legacy retry')
        expect(operation_exception.message).not_to include('modern retry')
      end
    end

    context 'when read is retried and retry fails' do
      include_context 'read operation'

      it_behaves_like 'failing retry'
      it_behaves_like 'modern retry'

      context 'legacy read' do
        let(:client) do
          subscribed_client.with(retry_reads: false, read_retry_interval: 0)
        end

        it_behaves_like 'failing retry'
        it_behaves_like 'legacy retry'
      end
    end

    context 'when write is retried and retry fails' do
      include_context 'write operation'

      it_behaves_like 'failing retry'
      it_behaves_like 'modern retry'

      context 'legacy write' do
        let(:client) do
          subscribed_client.with(retry_writes: false)
        end

        it_behaves_like 'failing retry'
        it_behaves_like 'legacy retry'
      end
    end
  end
end
