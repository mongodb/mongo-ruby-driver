# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2016-2020 MongoDB Inc.
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
      # @since 2.4.0
      class TopologyChanged < Mongo::Event::Base

        # @return [ Cluster::Topology ] previous_topology The previous topology.
        attr_reader :previous_topology

        # @return [ Cluster::Topology ] new_topology The new topology.
        attr_reader :new_topology

        # Create the event.
        #
        # @example Create the event.
        #   TopologyChanged.new(previous, new)
        #
        # @param [ Cluster::Topology ] previous_topology The previous topology.
        # @param [ Cluster::Topology ] new_topology The new topology.
        #
        # @since 2.4.0
        def initialize(previous_topology, new_topology)
          @previous_topology = previous_topology
          @new_topology = new_topology
        end

        # Returns a concise yet useful summary of the event.
        #
        # @return [ String ] String summary of the event.
        #
        # @note This method is experimental and subject to change.
        #
        # @since 2.7.0
        # @api experimental
        def summary
          "#<#{short_class_name}" +
          " prev=#{previous_topology.summary}" +
          " new=#{new_topology.summary}>"
        end
      end
    end
  end
end
