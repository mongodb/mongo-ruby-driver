module CommonShortcuts
  module ClassMethods
    # Declares a topology double, which is configured to accept summary
    # calls as those are used in SDAM event creation
    def declare_topology_double
      let(:topology) do
        double('topology').tap do |topology|
          allow(topology).to receive(:summary)
        end
      end
    end
  end

  module InstanceMethods
    def wait_for_all_servers(cluster)
      # Cluster waits for initial round of sdam until the primary
      # is discovered, which means by the time a connection is obtained
      # here some of the servers in the topology may still be unknown.
      # This messes with event expectations below. Therefore, wait for
      # all servers in the topology to be checked.
      #
      # This wait here assumes all addresses specified for the test
      # suite are for working servers of the cluster; if this is not
      # the case, this test will fail due to exceeding the general
      # test timeout eventually.
      while cluster.servers_list.any? { |server| server.unknown? }
        warn "Waiting for unknown servers in #{cluster.servers}"
        sleep 0.25
      end
    end

    def make_server(mode, options = {})
      tags = options[:tags] || {}
      average_round_trip_time = options[:average_round_trip_time] || 0

      ismaster = {
        'ismaster' => mode == :primary,
        'secondary' => mode == :secondary,
        'arbiterOnly' => mode == :arbiter,
        'isreplicaset' => mode == :ghost,
        'hidden' => mode == :other,
        'tags' => tags,
        'ok' => 1,
        'minWireVersion' => 2, 'maxWireVersion' => 8,
      }
      if [:primary, :secondary, :arbiter, :other].include?(mode)
        ismaster['setName'] = 'mongodb_set'
      end

      listeners = Mongo::Event::Listeners.new
      monitoring = Mongo::Monitoring.new
      address = options[:address]

      cluster = double('cluster')
      allow(cluster).to receive(:topology).and_return(topology)
      allow(cluster).to receive(:app_metadata)
      allow(cluster).to receive(:options).and_return({})
      server = Mongo::Server.new(address, cluster, monitoring, listeners, SpecConfig.instance.test_options)
      description = Mongo::Server::Description.new(address, ismaster, average_round_trip_time)
      server.tap do |s|
        allow(s).to receive(:description).and_return(description)
      end
    end
  end
end
