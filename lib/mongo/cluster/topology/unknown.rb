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
      module Unknown
        extend self

        # The display name for the topology.
        #
        # @since 2.0.0
        NAME = 'Unknown'.freeze

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

        # An unknown topology is not a replica set.
        #
        # @example Is the topology a replica set?
        #   Unknown.replica_set?
        #
        # @return [ false ] Always false.
        #
        # @since 2.0.0
        def replica_set?; false; end

        # Select appropriate servers for this topology.
        #
        # @example Select the servers.
        #   Unknown.servers(servers, 'test')
        #
        # @param [ Array<Server> ] servers The known servers.
        #
        # @raise [ Unknown ] Cannot select servers when the topology is
        #   unknown.
        #
        # @since 2.0.0
        def servers(servers, name = nil)
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
      end
    end
  end
end
