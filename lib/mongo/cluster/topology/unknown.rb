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
            log_debug([ "Mongos #{description.address.to_s} discovered." ])
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
        def initialize(options)
          @options = options
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

        # An unknown topology is not standalone.
        #
        # @example Is the topology standalone?
        #   Unknown.standalone?
        #
        # @return [ true ] Always false.
        #
        # @since 2.0.0
        def standalone?; false; end

        # An unknown topology is unknown.
        #
        # @example Is the topology unknown?
        #   Unknown.unknown?
        #
        # @return [ true ] Always true.
        #
        # @since 2.0.0
        def unknown?; true; end

        private

        def initialize_replica_set(description, servers)
          log_debug([ "Server #{description.address.to_s} discovered as primary." ])
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
