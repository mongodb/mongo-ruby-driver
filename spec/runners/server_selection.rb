# frozen_string_literal: true
# rubocop:todo all

module Mongo
  module ServerSelection
    module Read

      # Represents a Server Selection specification test.
      #
      # @since 2.0.0
      class Spec

        # Mapping of read preference modes.
        #
        # @since 2.0.0
        READ_PREFERENCES = {
          'Primary' => :primary,
          'Secondary' => :secondary,
          'PrimaryPreferred' => :primary_preferred,
          'SecondaryPreferred' => :secondary_preferred,
          'Nearest' => :nearest,
        }

        # @return [ String ] description The spec description.
        #
        # @since 2.0.0
        attr_reader :description

        # @return [ Hash ] read_preference The read preference to be used for selection.
        #
        # @since 2.0.0
        attr_reader :read_preference

        # @return [ Integer ] heartbeat_frequency The heartbeat frequency to be set on the client.
        #
        # @since 2.4.0
        attr_reader :heartbeat_frequency

        # @return [ Integer ] max_staleness The max_staleness.
        #
        # @since 2.4.0
        attr_reader :max_staleness

        # @return [ Array<Hash> ] eligible_servers The eligible servers before the latency
        #   window is taken into account.
        #
        # @since 2.0.0
        attr_reader :eligible_servers

        # @return [ Array<Hash> ] suitable_servers The set of servers matching all server
        #  selection logic. May be a subset of eligible_servers and/or candidate_servers.
        #
        # @since 2.0.0
        attr_reader :suitable_servers

        # @return [ Mongo::Cluster::Topology ] type The topology type.
        #
        # @since 2.0.0
        attr_reader :type

        # Instantiate the new spec.
        #
        # @param [ String ] test_path The path to the file.
        #
        # @since 2.0.0
        def initialize(test_path)
          @test = ::Utils.load_spec_yaml_file(test_path)
          @description = "#{@test['topology_description']['type']}: #{File.basename(test_path)}"
          @heartbeat_frequency = @test['heartbeatFrequencyMS'] / 1000 if @test['heartbeatFrequencyMS']
          @read_preference = @test['read_preference']
          @read_preference['mode'] = READ_PREFERENCES[@read_preference['mode']]
          @max_staleness = @read_preference['maxStalenessSeconds']
          @candidate_servers = @test['topology_description']['servers']
          @suitable_servers = @test['suitable_servers'] || []
          @in_latency_window = @test['in_latency_window'] || []
          @type = Mongo::Cluster::Topology.const_get(@test['topology_description']['type'])
        end

        # Does this spec expect a server to be found.
        #
        # @example Will a server be found with this spec.
        #   spec.server_available?
        #
        # @return [true, false] If a server will be found with this spec.
        #
        # @since 2.0.0
        def server_available?
          !in_latency_window.empty?
        end

        # Whether the test requires an error to be raised during server selection.
        #
        # @return [ true, false ] Whether the test expects an error.
        def error?
          @test['error']
        end

        # The subset of suitable servers that falls within the allowable latency
        #   window.
        # We have to correct for our server selection algorithm that adds the primary
        #  to the end of the list for SecondaryPreferred read preference mode.
        #
        # @example Get the list of suitable servers within the latency window.
        #   spec.in_latency_window
        #
        # @return [ Array<Hash> ] The servers within the latency window.
        #
        # @since 2.0.0
        def in_latency_window
          @in_latency_window
        end

        # The servers a topology would return as candidates for selection.
        #
        # @return [ Array<Hash> ] candidate_servers The candidate servers.
        #
        # @since 2.0.0
        def candidate_servers
          @candidate_servers
        end
      end
    end
  end
end

def define_server_selection_spec_tests(test_paths)
  # Linter insists that a server selection semaphore is present when
  # performing server selection.
  require_no_linting

  test_paths.each do |file|

    spec = Mongo::ServerSelection::Read::Spec.new(file)

    context(spec.description) do
      # Cluster needs a topology and topology needs a cluster...
      # This temporary cluster is used for topology construction.
      let(:temp_cluster) do
        double('temp cluster').tap do |cluster|
          allow(cluster).to receive(:servers_list).and_return([])
        end
      end

      let(:topology) do
        options = if spec.type <= Mongo::Cluster::Topology::ReplicaSetNoPrimary
          {replica_set_name: 'foo'}
        else
          {}
        end
        spec.type.new(options, monitoring, temp_cluster)
      end

      let(:monitoring) do
        Mongo::Monitoring.new(monitoring: false)
      end

      let(:listeners) do
        Mongo::Event::Listeners.new
      end

      let(:options) do
        if spec.heartbeat_frequency
          {server_selection_timeout: 0.1, heartbeat_frequency: spec.heartbeat_frequency}
        else
          {server_selection_timeout: 0.1}
        end
      end

      let(:cluster) do
        double('cluster').tap do |c|
          allow(c).to receive(:server_selection_semaphore)
          allow(c).to receive(:connected?).and_return(true)
          allow(c).to receive(:summary)
          allow(c).to receive(:topology).and_return(topology)
          allow(c).to receive(:single?).and_return(topology.single?)
          allow(c).to receive(:sharded?).and_return(topology.sharded?)
          allow(c).to receive(:replica_set?).and_return(topology.replica_set?)
          allow(c).to receive(:unknown?).and_return(topology.unknown?)
          allow(c).to receive(:options).and_return(options)
          allow(c).to receive(:scan!).and_return(true)
          allow(c).to receive(:app_metadata).and_return(app_metadata)
          allow(c).to receive(:heartbeat_interval).and_return(
            spec.heartbeat_frequency || Mongo::Server::Monitor::DEFAULT_HEARTBEAT_INTERVAL)
        end
      end

      # One of the spec test assertions is on the set of servers that are
      # eligible for selection without taking latency into account.
      # In the driver, latency is taken into account at various points during
      # server selection, hence there isn't a method that can be called to
      # retrieve the list of servers without accounting for latency.
      # Work around this by executing server selection with all servers set
      # to zero latency, when evaluating the candidate server set.
      let(:ignore_latency) { false }

      let(:candidate_servers) do
        spec.candidate_servers.collect do |server|
          features = double('features').tap do |feat|
            allow(feat).to receive(:max_staleness_enabled?).and_return(server['maxWireVersion'] && server['maxWireVersion'] >= 5)
            allow(feat).to receive(:check_driver_support!).and_return(true)
          end
          address = Mongo::Address.new(server['address'])
          Mongo::Server.new(address, cluster, monitoring, listeners,
            {monitoring_io: false}.update(options)
          ).tap do |s|
            allow(s).to receive(:average_round_trip_time) do
              if ignore_latency
                0
              elsif server['avg_rtt_ms']
                server['avg_rtt_ms'] / 1000.0
              end
            end
            allow(s).to receive(:tags).and_return(server['tags'])
            allow(s).to receive(:secondary?).and_return(server['type'] == 'RSSecondary')
            allow(s).to receive(:primary?).and_return(server['type'] == 'RSPrimary')
            allow(s).to receive(:mongos?).and_return(server['type'] == 'Mongos')
            allow(s).to receive(:standalone?).and_return(server['type'] == 'Standalone')
            allow(s).to receive(:unknown?).and_return(server['type'] == 'Unknown')
            allow(s).to receive(:connectable?).and_return(true)
            allow(s).to receive(:last_write_date).and_return(
              Time.at(server['lastWrite']['lastWriteDate']['$numberLong'].to_f / 1000)) if server['lastWrite']
            allow(s).to receive(:last_scan).and_return(
              Time.at(server['lastUpdateTime'].to_f / 1000))
            allow(s).to receive(:features).and_return(features)
            allow(s).to receive(:replica_set_name).and_return('foo')
          end
        end
      end

      let(:suitable_servers) do
        spec.suitable_servers.collect do |server|
          Mongo::Server.new(Mongo::Address.new(server['address']), cluster, monitoring, listeners,
            options.merge(monitoring_io: false))
        end
      end

      let(:in_latency_window) do
        spec.in_latency_window.collect do |server|
          Mongo::Server.new(Mongo::Address.new(server['address']), cluster, monitoring, listeners,
            options.merge(monitoring_io: false))
        end
      end

      let(:server_selector_definition) do
        { mode: spec.read_preference['mode'] }.tap do |definition|
          definition[:tag_sets] = spec.read_preference['tag_sets']
          definition[:max_staleness] = spec.max_staleness if spec.max_staleness
        end
      end

      let(:server_selector) do
        Mongo::ServerSelector.get(server_selector_definition)
      end

      let(:app_metadata) do
        Mongo::Server::AppMetadata.new({})
      end

      before do
        allow(cluster).to receive(:servers_list).and_return(candidate_servers)
        allow(cluster).to receive(:servers) do
          # Copy Cluster#servers definition because clusters is a double
          cluster.topology.servers(cluster.servers_list)
        end
        allow(cluster).to receive(:addresses).and_return(candidate_servers.map(&:address))
      end

      if spec.error?

        it 'Raises an InvalidServerPreference exception' do

          expect do
            server_selector.select_server(cluster)
          end.to raise_exception(Mongo::Error::InvalidServerPreference)
        end

      else

        if spec.server_available?

          it 'has non-empty suitable servers' do
            spec.suitable_servers.should be_a(Array)
            spec.suitable_servers.should_not be_empty
          end

          if spec.in_latency_window.length == 1

            it 'selects the expected server' do
              [server_selector.select_server(cluster)].should == in_latency_window
            end

          else

            it 'selects a server in the suitable list' do
              in_latency_window.should include(server_selector.select_server(cluster))
            end

            let(:expected_addresses) do
              in_latency_window.map(&:address).map(&:seed).sort
            end

            let(:actual_addresses) do
              server_selector.suitable_servers(cluster).map(&:address).map(&:seed).sort
            end

            it 'identifies expected suitable servers' do
              actual_addresses.should == expected_addresses
            end

          end

          context 'candidate servers without taking latency into account' do
            let(:ignore_latency) { true }

            let(:expected_addresses) do
              suitable_servers.map(&:address).map(&:seed).sort
            end

            let(:actual_addresses) do
              servers = server_selector.send(:suitable_servers, cluster)

              # The tests expect that only secondaries are "suitable" for
              # server selection with secondary preferred read preference.
              # In actuality, primaries are also suitable, and the driver
              # returns the primaries also. Remove primaries from the
              # actual set when read preference is secondary preferred.
              # HOWEVER, if a test ends up selecting a primary, then it
              # includes that primary into its suitable servers. Therefore
              # only remove primaries when the number of suitable servers
              # is greater than 1.
              servers.delete_if do |server|
                server_selector.is_a?(Mongo::ServerSelector::SecondaryPreferred) &&
                  server.primary? &&
                  servers.length > 1
              end

              # Since we remove the latency requirement, the servers
              # may be returned in arbitrary order.
              servers.map(&:address).map(&:seed).sort
            end

            it 'identifies expected suitable servers' do
              actual_addresses.should == expected_addresses
            end
          end

        else

          # Runner does not handle non-empty suitable servers with
          # no servers in latency window.
          it 'has empty suitable servers' do
            expect(spec.suitable_servers).to eq([])
          end

          it 'Raises a NoServerAvailable Exception' do
            expect do
              server_selector.select_server(cluster)
            end.to raise_exception(Mongo::Error::NoServerAvailable)
          end

        end
      end
    end
  end
end
