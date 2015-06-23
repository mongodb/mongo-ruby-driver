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

        # Handles notifying the cluster that a standalone was discovered.
        #
        # @since 2.0.6
        class StandaloneDiscovered
          include Event::Publisher

          # Instantiate the standalone discovered inspection.
          #
          # @example Instantiate the inspection.
          #   StandaloneDiscovered.new(listeners)
          #
          # @param [ Event::Listeners ] event_listeners The event listeners.
          #
          # @since 2.0.6
          def initialize(event_listeners)
            @event_listeners = event_listeners
          end

          # Run the standalone discovered inspection.
          #
          # @example Run the inspection.
          #   StandaloneDiscovered.run(description, {})
          #
          # @param [ Description ] description The server description.
          # @param [ Description ] updated The updated description.
          #
          # @since 2.0.6
          def run(description, updated)
            if !description.standalone? && updated.standalone?
              publish(Event::STANDALONE_DISCOVERED, updated)
            end
          end
        end
      end
    end
  end
end
