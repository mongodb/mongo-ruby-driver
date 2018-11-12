# Copyright (C) 2014-2018 MongoDB, Inc.
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
      OPTIONS = {
        replica_set: ReplicaSetNoPrimary,
        sharded: Sharded,
        direct: Single,
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
      # @return [ ReplicaSet, Sharded, Single ] The topology.
      #
      # @since 2.0.0
      # @api private
      def initial(cluster, monitoring, options)
        cls = if options.key?(:connect)
          OPTIONS.fetch(options[:connect].to_sym)
        elsif options.key?(:replica_set) || options.key?(:replica_set_name)
          ReplicaSetNoPrimary
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
