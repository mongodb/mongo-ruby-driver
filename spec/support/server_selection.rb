module Mongo
  module ServerSelection
    module Read

      # Represents a Server Selection specification test.
      #
      # @since 2.0.0
      class Spec

        # Mapping of topology description strings to topology type classes.
        #
        # @since 2.0.0
        TOPOLOGY_TYPES = {
          'ReplicaSetNoPrimary' => Mongo::Cluster::Topology::ReplicaSet,
          'ReplicaSetWithPrimary' => Mongo::Cluster::Topology::ReplicaSet,
          'Sharded' => Mongo::Cluster::Topology::Sharded,
          'Single' => Mongo::Cluster::Topology::Single,
          'Unknown' => Mongo::Cluster::Topology::Unknown
        }

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
        # @example Create the spec.
        #   Spec.new(file)
        #
        # @param [ String ] file The name of the file.
        #
        # @since 2.0.0
        def initialize(file)
          file = File.new(file)
          @test = YAML.load(ERB.new(file.read).result)
          file.close
          @description = "#{@test['topology_description']['type']}: #{File.basename(file)}"
          @heartbeat_frequency = @test['heartbeatFrequencyMS'] / 1000 if @test['heartbeatFrequencyMS']
          @read_preference = @test['read_preference']
          @read_preference['mode'] = READ_PREFERENCES[@read_preference['mode']]
          @max_staleness = @read_preference['maxStalenessSeconds']
          @candidate_servers = @test['topology_description']['servers']
          @suitable_servers = @test['suitable_servers'] || []
          @in_latency_window = @test['in_latency_window'] || []
          @type = TOPOLOGY_TYPES[@test['topology_description']['type']]
        end

        # Whether this spec describes a replica set.
        #
        # @example Determine if the spec describes a replica set.
        #   spec.replica_set?
        #
        # @return [true, false] If the spec describes a replica set.
        #
        # @since 2.0.0
        def replica_set?
          type == Mongo::Cluster::Topology::ReplicaSet
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

        # Is the max staleness setting invalid.
        #
        # @example Will the max staleness setting be valid with other options.
        #   spec.invalid_max_staleness?
        #
        # @return [ true, false ] If an error will be raised by the max staleness setting.
        #
        # @since 2.4.0
        def invalid_max_staleness?
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
          if read_preference['mode'] == :secondary_preferred && primary
            return @in_latency_window.push(primary).uniq
          end
          @in_latency_window
        end

        # The servers a topology would return as candidates for selection.
        #
        # @return [ Array<Hash> ] candidate_servers The candidate servers.
        #
        # @since 2.0.0
        def candidate_servers
          @candidate_servers.select { |s| s['type'] != 'Unknown' }
        end

        private

        def primary
          @candidate_servers.find { |s| s['type'] == 'RSPrimary' }
        end
      end
    end
  end
end
