# Copyright (C) 2009-2014 MongoDB, Inc.
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
      module Inspection

        # Handles inspecting the result of an ismaster command to determine the
        # server type and fire an event if changed.
        #
        # @since 2.0.0
        class ServerType

          # Run the server type inspection.
          #
          # @example Run the inspection.
          #   ServerType.run(description, {})
          #
          # @param [ Description ] description The server description.
          # @param [ Description ] updated The updated description.
          #
          # @since 2.0.0
          def self.run(description, updated)
            updated.server_type = Server::Type.determine(updated)
            if description.server_type != updated.server_type
              description.server_type = updated.server_type
              description.server.publish(
                Event::SERVER_TYPE_CHANGED,
                description.server.address.to_s,
                description.server_type
              )
            end
          end
        end
      end
    end
  end
end
