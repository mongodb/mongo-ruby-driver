require 'spec_helper'

describe 'Retryable writes tests' do

  let(:client) do
    authorized_client.with(retry_writes: true)
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

    it 'is reported on the server of the second attempt' do
      first_primary = client.cluster.next_primary

      client.cluster.servers_list.each do |server|
        server.monitor.stop!
      end

      ClusterTools.instance.direct_client_for_each_server do |client|
        client.use(:admin).database.command(fail_point_command)
      end

      # Retry should happen on a server other than the one used for
      # initial attempt
      expected_hosts = client.cluster.servers.reject do |server|
        server.address == first_primary.address
      end.map(&:address).map(&:host)

      begin
        collection.find(a: 1).to_a
      rescue Mongo::Error::OperationFailure => e
      else
        fail('Expected operation to fail')
      end

      puts e.message

      found = expected_hosts.any? do |host|
        e.message.include?(host)
      end
      expect(found).to be true

      expect(e.message).not_to include("on #{first_primary.address.seed}")

      current_primary = client.cluster.next_primary
    end
  end
end
