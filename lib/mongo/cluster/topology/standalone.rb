# Copyright (C) 2009-2014 MongoDB, Inc.
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

      # Defines behaviour for when a cluster is in standalone topology.
      #
      # @since 2.0.0
      class Standalone

        class << self

          # Select appropriate servers for this topology.
          #
          # @example Select the servers.
          #   Standalone.servers(servers, 'test')
          #
          # @param [ Array<Server> ] servers The known servers.
          #
          # @return [ Array<Server> ] The standalone servers.
          #
          # @since 2.0.0
          def servers(servers, name = nil)
            [ servers.detect{ |server| server.standalone? } ]
          end

          # A standalone topology is not sharded.
          #
          # @example Is the topology sharded?
          #   Standalone.sharded?
          #
          # @return [ false ] Always false.
          #
          # @since 2.0.0
          def sharded?; false; end

          # A standalone topology is standalone.
          #
          # @example Is the topology standalone?
          #   Standalone.standalone?
          #
          # @return [ true ] Always true.
          #
          # @since 2.0.0
          def standalone?; true; end
        end
      end
    end
  end
end
