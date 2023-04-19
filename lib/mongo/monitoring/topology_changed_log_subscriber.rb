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

    # Subscribes to Topology Changed events and logs them.
    #
    # @since 2.4.0
    class TopologyChangedLogSubscriber < SDAMLogSubscriber

      private

      def log_event(event)
        if event.previous_topology.class != event.new_topology.class
          log_debug(
            "Topology type '#{event.previous_topology.display_name}' changed to " +
            "type '#{event.new_topology.display_name}'."
          )
        else
          log_debug(
            "There was a change in the members of the '#{event.new_topology.display_name}' " +
              "topology."
          )
        end
      end
    end
  end
end
