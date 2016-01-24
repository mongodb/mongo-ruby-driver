# Copyright (C) 2016 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Mongo
  class Monitoring
    module Event

      # Event fired when the topology changes.
      #
      # @since 2.3.0
      class TopologyChanged

        # @return [ Topology ] topology The topology.
        attr_reader :topology

        # @return [ Cluster::Topology ] old_topology The old topology.
        attr_reader :old_topology

        # @return [ Cluster::Topology ] new_topology The new topology.
        attr_reader :new_topology

        # Create the event.
        #
        # @example Create the event.
        #   TopologyChanged.new(topology, old, new)
        #
        # @param [ Integer ] topology The topology.
        # @param [ Cluster::Topology ] old_topology The old topology.
        # @param [ Cluster::Topology ] new_topology The new topology.
        #
        # @since 2.3.0
        def initialize(topology, old_topology, new_topology)
          @topology = topology
          @old_topology = old_topology
          @new_topology = new_topology
        end
      end
    end
  end
end
