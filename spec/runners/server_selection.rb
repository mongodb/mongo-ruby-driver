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
          @test = YAML.load(File.read(test_path))
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
