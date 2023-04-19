# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'Failing retryable operations' do
  # Requirement for fail point
  min_server_fcv '4.0'

  let(:subscriber) { Mrss::EventSubscriber.new }

  let(:client_options) do
    {}
  end

  let(:client) do
    authorized_client.with(client_options).tap do |client|
      client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
    end
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
      ClusterTools.instance.direct_client_for_each_data_bearing_server do |client|
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

        ClusterTools.instance.direct_client_for_each_data_bearing_server do |client|
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
        subscriber.command_started_events('find')
      end
    end

    shared_context 'write operation' do
      let(:fail_point_command) do
        command = {
          configureFailPoint: 'failCommand',
          mode: {times: 2},
          data: {
            failCommands: ['insert'],
            errorCode: 11600,
          },
        }

        if ClusterConfig.instance.short_server_version >= '4.4'
          # Server versions 4.4 and newer will add the RetryableWriteError
          # label to all retryable errors, and the driver must not add the label
          # if it is not already present.
          command[:data][:errorLabels] = ['RetryableWriteError']
        end

        command
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

        #puts exception.message

        exception
      end

      let(:events) do
        subscriber.command_started_events('insert')
      end
    end

    shared_examples_for 'failing retry' do

      it 'indicates second attempt' do
        expect(operation_exception.message).to include('attempt 2')
        expect(operation_exception.message).not_to include('attempt 1')
        expect(operation_exception.message).not_to include('attempt 3')
      end

      it 'publishes two events' do
        operation_exception

        expect(events.length).to eq(2)
      end
    end

    shared_examples_for 'failing single attempt' do

      it 'does not indicate attempt' do
        expect(operation_exception.message).not_to include('attempt 1')
        expect(operation_exception.message).not_to include('attempt 2')
        expect(operation_exception.message).not_to include('attempt 3')
      end

      it 'publishes one event' do
        operation_exception

        expect(events.length).to eq(1)
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
        expect(operation_exception.message).not_to include('retries disabled')
      end
    end

    shared_examples_for 'legacy retry' do
      it 'indicates legacy retry' do
        expect(operation_exception.message).to include('legacy retry')
        expect(operation_exception.message).not_to include('modern retry')
        expect(operation_exception.message).not_to include('retries disabled')
      end
    end

    shared_examples_for 'disabled retry' do
      it 'indicates retries are disabled' do
        expect(operation_exception.message).to include('retries disabled')
        expect(operation_exception.message).not_to include('legacy retry')
        expect(operation_exception.message).not_to include('modern retry')
      end
    end

    context 'when read is retried and retry fails' do
      include_context 'read operation'

      context 'modern read retries' do
        require_wired_tiger_on_36

        let(:client_options) do
          {retry_reads: true}
        end

        it_behaves_like 'failing retry'
        it_behaves_like 'modern retry'
      end

      context 'legacy read retries' do
        let(:client_options) do
          {retry_reads: false, read_retry_interval: 0}
        end

        it_behaves_like 'failing retry'
        it_behaves_like 'legacy retry'
      end
    end

    context 'when read retries are disabled' do
      let(:client_options) do
        {retry_reads: false, max_read_retries: 0}
      end

      include_context 'read operation'

      it_behaves_like 'failing single attempt'
      it_behaves_like 'disabled retry'
    end

    context 'when write is retried and retry fails' do
      include_context 'write operation'

      context 'modern write retries' do
        require_wired_tiger_on_36

        let(:client_options) do
          {retry_writes: true}
        end

        it_behaves_like 'failing retry'
        it_behaves_like 'modern retry'
      end

      context 'legacy write' do
        let(:client_options) do
          {retry_writes: false}
        end

        it_behaves_like 'failing retry'
        it_behaves_like 'legacy retry'
      end
    end
  end
end
