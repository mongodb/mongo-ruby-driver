# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2021 MongoDB Inc.
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

      # Defines behavior for when a cluster is in load-balanced topology.
      class LoadBalanced < Base

        # The display name for the topology.
        NAME = 'LoadBalanced'.freeze

        # Get the display name.
        #
        # @return [ String ] The display name.
        def display_name
          self.class.name.gsub(/.*::/, '')
        end

        # @note This method is experimental and subject to change.
        #
        # @api experimental
        def summary
          details = server_descriptions.keys.join(',')
          "#{display_name}[#{details}]"
        end

        # Determine if the topology would select a readable server for the
        # provided candidates and read preference.
        #
        # @param [ Cluster ] cluster The cluster.
        # @param [ ServerSelector ] server_selector The server
        #   selector.
        #
        # @return [ true ] A standalone always has a readable server.
        def has_readable_server?(cluster, server_selector = nil); true; end

        # Determine if the topology would select a writable server for the
        # provided candidates.
        #
        # @param [ Cluster ] cluster The cluster.
        #
        # @return [ true ] A standalone always has a writable server.
        def has_writable_server?(cluster); true; end

        # Returns whether this topology is one of the replica set ones.
        #
        # @return [ false ] Always false.
        def replica_set?; false; end

        # Select appropriate servers for this topology.
        #
        # @param [ Array<Server> ] servers The known servers.
        #
        # @return [ Array<Server> ] All of the known servers.
        def servers(servers, name = nil)
          servers
        end

        # Returns whether this topology is sharded.
        #
        # @return [ false ] Always false.
        def sharded?; false; end

        # Returns whether this topology is Single.
        #
        # @return [ true ] Always false.
        def single?; false; end

        # Returns whether this topology is Unknown.
        #
        # @return [ false ] Always false.
        def unknown?; false; end

        private

        def validate_options(options, cluster)
          if cluster.servers_list.length > 1
            raise ArgumentError, "Cannot instantiate a load-balanced topology with more than one server in the cluster: #{cluster.servers_list.map(&:address).map(&:seed).join(', ')}"
          end

          super(options, cluster)
        end
      end
    end
  end
end
