require 'spec_helper'

describe 'Server::Monitor' do

  let(:client) do
    new_local_client([ClusterConfig.instance.primary_address_str],
      SpecConfig.instance.test_options.merge(SpecConfig.instance.auth_options.merge(
        heartbeat_frequency: 1)))
  end

  it 'refreshes server descriptions in background', retry: 3 do
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
end
