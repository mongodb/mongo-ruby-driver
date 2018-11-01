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
    module Topology

      # Defines behaviour for when a cluster is in sharded topology.
      #
      # @since 2.0.0
      class Sharded < Base
        include Monitoring::Publishable

        # The display name for the topology.
        #
        # @since 2.0.0
        NAME = 'Sharded'.freeze

        # Get the display name.
        #
        # @example Get the display name.
        #   Sharded.display_name
        #
        # @return [ String ] The display name.
        #
        # @since 2.0.0
        def display_name
          self.class.name.gsub(/.*::/, '')
        end

        # @note This method is experimental and subject to change.
        #
        # @api experimental
        # @since 2.7.0
        def summary
          display_name.gsub(' ', '')
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
        # @return [ Sharded ] The topology.
        def elect_primary(description, servers); self; end

        # Determine if the topology would select a readable server for the
        # provided candidates and read preference.
        #
        # @example Is a readable server present?
        #   topology.has_readable_server?(cluster, server_selector)
        #
        # @param [ Cluster ] cluster The cluster.
        # @param [ ServerSelector ] server_selector The server
        #   selector.
        #
        # @return [ true ] A Sharded cluster always has a readable server.
        #
        # @since 2.4.0
        def has_readable_server?(cluster, server_selector = nil); true; end

        # Determine if the topology would select a writable server for the
        # provided candidates.
        #
        # @example Is a writable server present?
        #   topology.has_writable_server?(servers)
        #
        # @param [ Cluster ] cluster The cluster.
        #
        # @return [ true ] A Sharded cluster always has a writable server.
        #
        # @since 2.4.0
        def has_writable_server?(cluster); true; end

        # A sharded topology is not a replica set.
        #
        # @example Is the topology a replica set?
        #   Sharded.replica_set?
        #
        # @return [ false ] Always false.
        #
        # @since 2.0.0
        def replica_set?; false; end

        # Sharded topologies have no replica set name.
        #
        # @example Get the replica set name.
        #   sharded.replica_set_name
        #
        # @return [ nil ] Always nil.
        #
        # @since 2.0.0
        def replica_set_name; nil; end

        # Select appropriate servers for this topology.
        #
        # @example Select the servers.
        #   Sharded.servers(servers)
        #
        # @param [ Array<Server> ] servers The known servers.
        #
        # @return [ Array<Server> ] The mongos servers.
        #
        # @since 2.0.0
        def servers(servers)
          servers.select{ |server| server.mongos? }
        end

        # A sharded topology is sharded.
        #
        # @example Is the topology sharded?
        #   Sharded.sharded?
        #
        # @return [ true ] Always true.
        #
        # @since 2.0.0
        def sharded?; true; end

        # A sharded topology is not single.
        #
        # @example Is the topology single?
        #   Sharded.single?
        #
        # @return [ false ] Always false.
        #
        # @since 2.0.0
        def single?; false; end

        # A sharded topology is not unknown.
        #
        # @example Is the topology unknown?
        #   Sharded.unknown?
        #
        # @return [ false ] Always false.
        #
        # @since 2.0.0
        def unknown?; false; end

        # Notify the topology that a member was discovered.
        #
        # @example Notify the cluster that a member was discovered.
        #   topology.member_discovered
        #
        # @since 2.4.0
        def member_discovered; end;

        private

        def remove_self?(description, server)
          description.is_server?(server) && !(description.mongos? || description.unknown?)
        end
      end
    end
  end
end
