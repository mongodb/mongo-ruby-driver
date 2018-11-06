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

      # Defines behaviour for when a cluster is in single topology.
      #
      # @since 2.0.0
      class Single < Base

        # The display name for the topology.
        #
        # @since 2.0.0
        NAME = 'Single'.freeze

        # Get the display name.
        #
        # @example Get the display name.
        #   Single.display_name
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
          display_name
        end

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
        # @return [ true ] A standalone always has a readable server.
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
        # @return [ true ] A standalone always has a writable server.
        #
        # @since 2.4.0
        def has_writable_server?(cluster); true; end

        # A single topology is not a replica set.
        #
        # @example Is the topology a replica set?
        #   Single.replica_set?
        #
        # @return [ false ] Always false.
        #
        # @since 2.0.0
        def replica_set?; false; end

        # Single topologies have no replica set name.
        #
        # @example Get the replica set name.
        #   single.replica_set_name
        #
        # @return [ nil ] Always nil.
        #
        # @since 2.0.0
        def replica_set_name; nil; end

        # Select appropriate servers for this topology.
        #
        # @example Select the servers.
        #   Single.servers(servers, 'test')
        #
        # @param [ Array<Server> ] servers The known servers.
        #
        # @return [ Array<Server> ] The single servers.
        #
        # @since 2.0.0
        def servers(servers, name = nil)
          [ servers.detect { |server| !server.unknown? } ]
        end

        # A single topology is not sharded.
        #
        # @example Is the topology sharded?
        #   Single.sharded?
        #
        # @return [ false ] Always false.
        #
        # @since 2.0.0
        def sharded?; false; end

        # A single topology is single.
        #
        # @example Is the topology single?
        #   Single.single?
        #
        # @return [ true ] Always true.
        #
        # @since 2.0.0
        def single?; true; end

        # An single topology is not unknown.
        #
        # @example Is the topology unknown?
        #   Single.unknown?
        #
        # @return [ false ] Always false.
        #
        # @since 2.0.0
        def unknown?; false; end
      end
    end
  end
end
