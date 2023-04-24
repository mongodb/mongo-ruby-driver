# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'Server::Monitor' do
  require_topology :single, :replica_set, :sharded

  let(:client) do
    new_local_client([ClusterConfig.instance.primary_address_str],
      SpecConfig.instance.test_options.merge(SpecConfig.instance.auth_options.merge(
        monitor_options)))
  end

  let(:monitor_options) do
    {heartbeat_frequency: 1}
  end

  retry_test
  it 'refreshes server descriptions in background' do
    server = client.cluster.next_primary

    expect(server.description).not_to be_unknown

    server.unknown!

    # This is racy, especially in JRuby, because the monitor may have
    # already run and updated the description. Because of this we retry
    # the test a few times.
    expect(server.description).to be_unknown

    # Wait for background thread to update the description
    sleep 1.5

    expect(server.description).not_to be_unknown
  end

  context 'server-pushed hello' do
    min_server_fcv '4.4'
    require_topology :replica_set

    let(:monitor_options) do
      {heartbeat_frequency: 20}
    end

    it 'updates server description' do
      starting_primary_address = client.cluster.next_primary.address

      ClusterTools.instance.step_down

      sleep 2

      new_primary_address = client.cluster.next_primary.address
      new_primary_address.should_not == starting_primary_address
    end
  end
end
