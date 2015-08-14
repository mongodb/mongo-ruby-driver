# Copyright (C) 2015 MongoDB, Inc.
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
    module Topology

      # Defines behaviour for when a cluster is in an unknown state.
      #
      # @since 2.0.0
      class Unknown
        include Loggable

        # The display name for the topology.
        #
        # @since 2.0.0
        NAME = 'Unknown'.freeze

        # @return [ Hash ] options The options.
        attr_reader :options

        # Get the display name.
        #
        # @example Get the display name.
        #   Unknown.display_name
        #
        # @return [ String ] The display name.
        #
        # @since 2.0.0
        def display_name
          NAME
        end

        # Elect a primary server within this topology.
        #
        # @example Elect a primary server.
        #   topology.elect_primary(description, servers)
        #
        # @param [ Server::Description ] description The description of the
        #   elected primary.
        # @param [ Array<Server> ] servers The list of known servers to the
        #   cluster.
        #
        # @return [ Sharded, ReplicaSet ] The new topology.
        def elect_primary(description, servers)
          if description.mongos?
            log_debug("Mongos #{description.address.to_s} discovered.")
            Sharded.new(options)
          else
            initialize_replica_set(description, servers)
          end
        end

        # Initialize the topology with the options.
        #
        # @example Initialize the topology.
        #   Unknown.new(options)
        #
        # @param [ Hash ] options The options.
        #
        # @since 2.0.0
        def initialize(options, seeds = [])
          @options = options
          @seeds = seeds
        end

        # An unknown topology is not a replica set.
        #
        # @example Is the topology a replica set?
        #   Unknown.replica_set?
        #
        # @return [ false ] Always false.
        #
        # @since 2.0.0
        def replica_set?; false; end

        # Unknown topologies have no replica set name.
        #
        # @example Get the replica set name.
        #   unknown.replica_set_name
        #
        # @return [ nil ] Always nil.
        #
        # @since 2.0.0
        def replica_set_name; nil; end

        # Select appropriate servers for this topology.
        #
        # @example Select the servers.
        #   Unknown.servers(servers)
        #
        # @param [ Array<Server> ] servers The known servers.
        #
        # @raise [ Unknown ] Cannot select servers when the topology is
        #   unknown.
        #
        # @since 2.0.0
        def servers(servers)
          []
        end

        # An unknown topology is not sharded.
        #
        # @example Is the topology sharded?
        #   Unknown.sharded?
        #
        # @return [ false ] Always false.
        #
        # @since 2.0.0
        def sharded?; false; end

        # An unknown topology is not single.
        #
        # @example Is the topology single?
        #   Unknown.single?
        #
        # @return [ true ] Always false.
        #
        # @since 2.0.0
        def single?; false; end

        # An unknown topology is unknown.
        #
        # @example Is the topology unknown?
        #   Unknown.unknown?
        #
        # @return [ true ] Always true.
        #
        # @since 2.0.0
        def unknown?; true; end

        # Whether a server description's hosts may be added to the cluster.
        #
        # @example Check if a description's hosts may be added to the cluster.
        #   topology.add_hosts?(description, servers)
        #
        # @param [ Mongo::Server::Description ] description The description.
        # @param [ Array<Mongo::Server> ] servers The cluster servers.
        #
        # @return [ true, false ] Whether a description's hosts may be added.
        #
        # @since 2.0.6
        def add_hosts?(description, servers)
          !(description.unknown? || description.ghost?)
        end

        # Whether a description can be used to remove hosts from the cluster.
        #
        # @example Check if a description can be used to remove hosts from the cluster.
        #   topology.remove_hosts?(description)
        #
        # @param [ Mongo::Server::Description ] description The description.
        #
        # @return [ true, false ] Whether hosts may be removed from the cluster.
        #
        # @since 2.0.6
        def remove_hosts?(description)
          description.standalone?
        end

        # Whether a specific server in the cluster can be removed, given a description.
        #
        # @example Check if a specific server can be removed from the cluster.
        #   topology.remove_server?(description, server)
        #
        # @param [ Mongo::Server::Description ] description The description.
        # @param [ Mongo::Serve ] server The server in question.
        #
        # @return [ true, false ] Whether the server can be removed from the cluster.
        #
        # @since 2.0.6
        def remove_server?(description, server)
          description.standalone? && description.is_server?(server)
        end

        # Notify the topology that a standalone was discovered.
        #
        # @example Notify the topology that a standalone was discovered.
        #   topology.standalone_discovered
        #
        # @return [ Topology::Unknown, Topology::Single ] Either self or a
        #   new Single topology.
        #
        # @since 2.0.6
        def standalone_discovered
          if @seeds.size == 1
            Single.new(options, @seeds)
          else
            self
          end
        end

        private

        def initialize_replica_set(description, servers)
          log_debug(
            "Server #{description.address.to_s} discovered as primary in replica set: " +
            "'#{description.replica_set_name}'. Changing topology to replica set."
          )
          servers.each do |server|
            if server.standalone? && server.address != description.address
              server.description.unknown!
            end
          end
          ReplicaSet.new(options.merge(:replica_set => description.replica_set_name))
        end
      end
    end
  end
end
