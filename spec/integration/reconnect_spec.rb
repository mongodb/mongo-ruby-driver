# frozen_string_literal: true
# rubocop:todo all

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

  context 'non-lb' do
    require_topology :single, :replica_set, :sharded

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
  end

  context 'lb' do
    require_topology :load_balanced

    it 'does not recreate monitor thread' do
      thread = client.cluster.servers.first.monitor.instance_variable_get('@thread')
      expect(thread).to be nil

      client.reconnect

      new_thread = client.cluster.servers.first.monitor.instance_variable_get('@thread')
      expect(new_thread).to be nil
    end
  end

  context 'with min_pool_size > 0' do
    # This test causes live threads errors in jruby in other tests.
    fails_on_jruby

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

  context 'SRV monitor thread' do
    require_external_connectivity

    let(:uri) do
      "mongodb+srv://test1.test.build.10gen.cc/?tls=#{SpecConfig.instance.ssl?}"
    end

    # Debug logging to troubleshoot failures in Evergreen
    let(:logger) do
      Logger.new(STDERR). tap do |logger|
        logger.level = :debug
      end
    end

    let(:client) do
      new_local_client(uri, SpecConfig.instance.monitoring_options.merge(
        server_selection_timeout: 3.86, logger: logger))
    end

    let(:wait_for_discovery) do
      client.cluster.next_primary
    end

    let(:wait_for_discovery_again) do
      client.cluster.next_primary
    end

    shared_examples_for 'recreates SRV monitor' do
      # JRuby produces this error:
      # RSpec::Expectations::ExpectationNotMetError: expected nil to respond to `alive?`
      # for this assertion:
      # expect(thread).not_to be_alive
      # This is bizarre because if thread was nil, the earlier call to
      # thread.kill should've similarly failed, but it doesn't.
      fails_on_jruby

      it 'recreates SRV monitor' do
        wait_for_discovery

        expect(client.cluster.topology).to be_a(expected_topology_cls)
        thread = client.cluster.srv_monitor.instance_variable_get('@thread')
        expect(thread).to be_alive

        thread.kill
        # context switch to let the thread get killed
        sleep 0.1
        expect(thread).not_to be_alive

        client.reconnect

        wait_for_discovery_again

        new_thread = client.cluster.srv_monitor.instance_variable_get('@thread')
        expect(new_thread).not_to eq(thread)
        expect(new_thread).to be_alive
      end
    end

    context 'in sharded topology' do
      require_topology :sharded
      require_default_port_deployment
      require_multi_mongos

      let(:expected_topology_cls) { Mongo::Cluster::Topology::Sharded }

      it_behaves_like 'recreates SRV monitor'
    end

    context 'in unknown topology' do
      require_external_connectivity

      # JRuby apparently does not implement non-blocking UDP I/O which is used
      # by RubyDNS:
      # NotImplementedError: recvmsg_nonblock is not implemented
      fails_on_jruby

      let(:uri) do
        "mongodb+srv://test-fake.test.build.10gen.cc/"
      end

      let(:client) do
        ClientRegistry.instance.register_local_client(
          Mongo::Client.new(uri,
            timeout: 5,
            connect_timeout: 5,
            server_selection_timeout: 3.89,
            resolv_options: {
              nameserver: 'localhost',
              nameserver_port: [['localhost', 5300], ['127.0.0.1', 5300]],
            },
            logger: logger))
      end

      let(:expected_topology_cls) { Mongo::Cluster::Topology::Unknown }

      let(:wait_for_discovery) do
        # Since the entire test is done in unknown topology, we cannot use
        # next_primary to wait for the client to discover the topology.
        sleep 5
      end

      let(:wait_for_discovery_again) do
        sleep 5
      end

      around do |example|
        require 'support/dns'

        rules = [
          ['_mongodb._tcp.test-fake.test.build.10gen.cc', :srv,
            [0, 0, 2799, 'localhost.test.build.10gen.cc'],
          ],
        ]

        mock_dns(rules) do
          example.run
        end
      end

      it_behaves_like 'recreates SRV monitor'
    end
  end
end
