require 'spec_helper'

describe 'Retryable writes tests' do
  # Requirement for fail point
  min_server_fcv '4.0'

  let(:client) do
    subscribed_client
  end

  let(:collection) do
    client['retryable-writes-error-spec']
  end

  context 'when retry fails' do
    require_topology :replica_set

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
      client['retryable-writes-error-spec', read: {mode: :secondary_preferred}]
    end

    let(:events) do
      events = EventSubscriber.command_started_events('find')
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

    let(:perform_read) do
      client.cluster.servers_list.each do |server|
        server.monitor.stop!
      end

      ClusterTools.instance.direct_client_for_each_server do |client|
        client.use(:admin).database.command(fail_point_command)
      end

      begin
        collection.find(a: 1).to_a
      rescue Mongo::Error::OperationFailure => @exception
      else
        fail('Expected operation to fail')
      end

      puts @exception.message

      expect(events.length).to eq(2)
      expect(events.first.address.seed).not_to eq(events.last.address.seed)
    end

    it 'is reported on the server of the second attempt' do
      perform_read

      expect(@exception.message).not_to include(first_server.address.seed)
      expect(@exception.message).to include(second_server.address.seed)
    end

    it 'marks servers used in both attempts unknown' do
      perform_read

      expect(first_server).to be_unknown

      expect(second_server).to be_unknown
    end
  end
end
