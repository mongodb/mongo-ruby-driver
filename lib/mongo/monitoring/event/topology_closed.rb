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

      # Event fired when the topology closes.
      #
      # @since 2.4.0
      class TopologyClosed < Mongo::Event::Base

        # @return [ Topology ] topology The topology.
        attr_reader :topology

        # Create the event.
        #
        # @example Create the event.
        #   TopologyClosed.new(topology)
        #
        # @param [ Integer ] topology The topology.
        #
        # @since 2.4.0
        def initialize(topology)
          @topology = topology
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
          " topology=#{topology.summary}>"
        end
      end
    end
  end
end
