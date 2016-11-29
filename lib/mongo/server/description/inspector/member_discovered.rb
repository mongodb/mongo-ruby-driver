# Copyright (C) 2015 MongoDB, Inc.
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
  class Server
    class Description
      class Inspector

        # Handles inspecting the result of an ismaster command to check if this
        # a server is a member of a known topology.
        #
        # @since 2.4.0
        class MemberDiscovered
          include Event::Publisher

          # Instantiate the member discovered inspection.
          #
          # @example Instantiate the inspection.
          #   MemberDiscovered.new(listeners)
          #
          # @param [ Event::Listeners ] event_listeners The event listeners.
          #
          # @since 2.4.0
          def initialize(event_listeners)
            @event_listeners = event_listeners
          end

          # Run the member discovered inspection.
          #
          # @example Run the inspection.
          #   MemberDiscovered.run(description, {})
          #
          # @param [ Description ] description The server description.
          # @param [ Description ] updated The updated description.
          #
          # @since 2.4.0
          def run(description, updated)
            if (!description.primary? && updated.primary?) ||
                (!description.mongos? && updated.mongos?) ||
                (description.unknown? && !updated.unknown?)
              publish(Event::MEMBER_DISCOVERED, description, updated)
            end
          end
        end
      end
    end
  end
end
