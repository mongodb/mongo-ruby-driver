module Mongo
  module ServerSelection
    module Read

      # Represents a specification.
      #
      # @since 2.0.0
      class Spec

        # Mapping of topology description strings to topology type classes.
        TOPOLOGY_TYPES = {
          'ReplicaSetNoPrimary' => Mongo::Cluster::Topology::ReplicaSet,
          'ReplicaSetWithPrimary' => Mongo::Cluster::Topology::ReplicaSet,
          'Sharded' => Mongo::Cluster::Topology::Sharded,
          'Single' => Mongo::Cluster::Topology::Standalone,
          'Unknown' => Mongo::Cluster::Topology::Unknown
        }

        # Mapping of read preference modes.
        READ_PREFERENCES = {
          'Primary' => :primary,
          'Secondary' => :secondary,
          'PrimaryPreferred' => :primary_preferred,
          'SecondaryPreferred' => :secondary_preferred,
          'Nearest' => :nearest,
        }

        # @return [ String ] description The spec description.
        attr_reader :description

        # @return [ Hash ] read_preference The read preference to be used for selection.
        attr_reader :read_preference

        # @return [ Array<Hash> ] candidate_servers The candidate servers.
        attr_reader :candidate_servers

        # @return [ Array<Hash> ] eligible_servers The eligible servers before latency
        #   window is taken into account.
        attr_reader :eligible_servers

        # @return [ Array<Hash> ] suitable_servers The set of servers matching all server
        #  selection logic. May be a subset of eligible_servers and/or candidate_servers
        attr_reader :suitable_servers

        # @return [ Array<Hash> ] in_latency_window The subset of suitable servers that falls
        #  within the allowable latency window.
        attr_reader :in_latency_window

        # @return [ Mongo::Cluster::Topology ] type The topology type.
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
          @test = YAML.load(ERB.new(File.new(file).read).result)
          @description = file#File.basename(file)
          @read_preference = @test['read_preference']
          @read_preference['mode'] = READ_PREFERENCES[@read_preference['mode']]
          @candidate_servers = @test['candidate_servers']
          @eligible_servers = @test['eligible_servers']
          @suitable_servers = @test['suitable_servers']
          @in_latency_window = @test['in_latency_window']
          @type = TOPOLOGY_TYPES[@test['topology_description']['type']]
        end
      end
    end
  end
end
