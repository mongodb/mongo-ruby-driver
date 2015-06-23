# Copyright (C) 2014-2015 MongoDB, Inc.
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

require 'mongo/cluster/topology/replica_set'
require 'mongo/cluster/topology/sharded'
require 'mongo/cluster/topology/single'
require 'mongo/cluster/topology/unknown'

module Mongo
  class Cluster

    # Defines behaviour for getting servers.
    #
    # @since 2.0.0
    module Topology
      extend self

      # The 2 various topologies for server selection.
      #
      # @since 2.0.0
      OPTIONS = {
        replica_set: ReplicaSet,
        sharded: Sharded,
        direct: Single
      }

      # Get the initial cluster topology for the provided options.
      #
      # @example Get the initial cluster topology.
      #   Topology.initial(topology: :replica_set)
      #
      # @param [ Array<String> ] seeds The addresses of the configured servers.
      # @param [ Hash ] options The cluster options.
      #
      # @return [ ReplicaSet, Sharded, Single ] The topology.
      #
      # @since 2.0.0
      def initial(seeds, options)
        if options.has_key?(:connect)
          OPTIONS.fetch(options[:connect]).new(options, seeds)
        elsif options.has_key?(:replica_set)
          ReplicaSet.new(options, seeds)
        else
          Unknown.new(options, seeds)
        end
      end
    end
  end
end
