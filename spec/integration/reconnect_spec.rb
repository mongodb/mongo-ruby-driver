require 'spec_helper'

describe 'Client after reconnect' do
  let(:client) { authorized_client }

  it 'is a functioning client' do
    client['test'].insert_one('testk' => 'testv')

    client.reconnect

    doc = client['test'].find('testk' => 'testv').first
    expect(doc).not_to be_nil
    expect(doc['testk']).to eq('testv')
  end

  it 'recreates monitor thread' do
    thread = client.cluster.servers.first.monitor.instance_variable_get('@thread')
    expect(thread).to be_alive

    thread.kill
    # context switch to let the thread get killed
    sleep 0.1
    expect(thread).not_to be_alive

    client.reconnect

    new_thread = client.cluster.servers.first.monitor.instance_variable_get('@thread')
    expect(new_thread).not_to eq(thread)
    expect(new_thread).to be_alive
  end

  context 'with min_pool_size > 0' do
    let(:client) { authorized_client.with(min_pool_size: 1) }

    it 'recreates connection pool populator thread' do
      server = client.cluster.next_primary
      thread = server.pool.populator.instance_variable_get('@thread')
      expect(thread).to be_alive

      thread.kill
      # context switch to let the thread get killed
      sleep 0.1
      expect(thread).not_to be_alive

      client.reconnect

      new_server = client.cluster.next_primary
      new_thread = new_server.pool.populator.instance_variable_get('@thread')
      expect(new_thread).not_to eq(thread)
      expect(new_thread).to be_alive
    end
  end

  context 'in sharded topology' do
    require_topology :sharded
    require_default_port_deployment
    require_multi_shard

    let(:uri) do
      "mongodb+srv://test1.test.build.10gen.cc/?tls=#{SpecConfig.instance.ssl?}&tlsInsecure=true".tap do |uri|
        puts "Constructed URI: #{uri}"
      end
    end

    # Debug logging to troubleshoot failures in Evergreen
    let(:logger) do
      Logger.new(STDERR). tap do |logger|
        logger.level = :debug
      end
    end

    let(:client) do
      ClientRegistry.instance.register_local_client(
        Mongo::Client.new(uri, server_selection_timeout: 3.86,
          logger: logger))
    end

    it 'recreates srv monitor' do
      client.cluster.next_primary
      expect(client.cluster.topology).to be_a(Mongo::Cluster::Topology::Sharded)
      thread = client.cluster.srv_monitor.instance_variable_get('@thread')
      expect(thread).to be_alive

      thread.kill
      # context switch to let the thread get killed
      sleep 0.1
      expect(thread).not_to be_alive

      client.reconnect

      client.cluster.next_primary
      new_thread = client.cluster.srv_monitor.instance_variable_get('@thread')
      expect(new_thread).not_to eq(thread)
      expect(new_thread).to be_alive
    end
  end
end
