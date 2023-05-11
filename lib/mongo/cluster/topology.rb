# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2014-2020 MongoDB Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Mongo
  class Cluster
    # Defines behavior for getting servers.
    #
    # Topologies are associated with their clusters - for example, a
    # ReplicaSet topology contains the replica set name. A topology
    # object therefore cannot be used with multiple cluster objects.
    #
    # At the same time, topology objects do not know anything about
    # specific *servers* in a cluster, despite what their constructor
    # may suggest. Which means, in particular, that topology change events
    # require the application to maintain cluster references on its own
    # if it wants to track server changes within a replica set.
    #
    # @since 2.0.0
    module Topology
      extend self
    end
  end
end

require 'mongo/cluster/topology/base'
require 'mongo/cluster/topology/no_replica_set_options'
require 'mongo/cluster/topology/load_balanced'
require 'mongo/cluster/topology/replica_set_no_primary'
require 'mongo/cluster/topology/replica_set_with_primary'
require 'mongo/cluster/topology/sharded'
require 'mongo/cluster/topology/single'
require 'mongo/cluster/topology/unknown'

module Mongo
  class Cluster
    module Topology
      # The various topologies for server selection.
      #
      # @since 2.0.0
      # @api private
      OPTIONS = {
        direct: Single,
        load_balanced: LoadBalanced,
        replica_set: ReplicaSetNoPrimary,
        sharded: Sharded,
      }.freeze

      # Get the initial cluster topology for the provided options.
      #
      # @example Get the initial cluster topology.
      #   Topology.initial(topology: :replica_set)
      #
      # @param [ Cluster ] cluster The cluster.
      # @param [ Monitoring ] monitoring The monitoring.
      # @param [ Hash ] options The cluster options.
      #
      # @option options [ true | false ] :direct_connection Whether to connect
      #   directly to the specified seed, bypassing topology discovery. Exactly
      #   one seed must be provided.
      # @option options [ Symbol ] :connect Deprecated - use :direct_connection
      #   option instead of this option. The connection method to use. This
      #   forces the cluster to behave in the specified way instead of
      #   auto-discovering. One of :direct, :replica_set, :sharded,
      #   :load_balanced. If :connect is set to :load_balanced, the driver
      #   will behave as if the server is a load balancer even if it isn't
      #   connected to a load balancer.
      # @option options [ true | false ] :load_balanced Whether to expect to
      #   connect to a load balancer.
      # @option options [ Symbol ] :replica_set The name of the replica set to
      #   connect to. Servers not in this replica set will be ignored.
      #
      # @return [ ReplicaSet, Sharded, Single, LoadBalanced ] The topology.
      #
      # @since 2.0.0
      # @api private
      def initial(cluster, monitoring, options)
        connect = options[:connect]&.to_sym
        cls = if options[:direct_connection]
          if connect && connect != :direct
            raise ArgumentError, "Conflicting topology options: direct_connection=true and connect=#{connect}"
          end
          if options[:load_balanced]
            raise ArgumentError, "Conflicting topology options: direct_connection=true and load_balanced=true"
          end
          Single
        elsif options[:direct_connection] == false && connect && connect == :direct
          raise ArgumentError, "Conflicting topology options: direct_connection=false and connect=#{connect}"
        elsif connect && connect != :load_balanced
          if options[:load_balanced]
            raise ArgumentError, "Conflicting topology options: connect=#{options[:connect].inspect} and load_balanced=true"
          end
          OPTIONS.fetch(options[:connect].to_sym)
        elsif options.key?(:replica_set) || options.key?(:replica_set_name)
          if options[:load_balanced]
            raise ArgumentError, "Conflicting topology options: replica_set/replica_set_name and load_balanced=true"
          end
          ReplicaSetNoPrimary
        elsif options[:load_balanced] || connect == :load_balanced
          LoadBalanced
        else
          Unknown
        end
        # Options here are client/cluster/server options.
        # In particular the replica set name key is different for
        # topology.
        # If replica_set_name is given (as might be internally by driver),
        # use that key.
        # Otherwise (e.g. options passed down from client),
        # move replica_set to replica_set_name.
        if (cls <= ReplicaSetNoPrimary || cls == Single) && !options[:replica_set_name]
          options = options.dup
          options[:replica_set_name] = options.delete(:replica_set)
        end
        cls.new(options, monitoring, cluster)
      end
    end
  end
end
