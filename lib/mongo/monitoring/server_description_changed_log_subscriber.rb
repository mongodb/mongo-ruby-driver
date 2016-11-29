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

    # Subscribes to Server Description Changed events and logs them.
    #
    # @since 2.4.0
    class ServerDescriptionChangedLogSubscriber < SDAMLogSubscriber

      private

      def log_event(event)
        log_debug(
          "Server description for #{event.address} changed from " +
          "'#{event.previous_description.server_type}' to '#{event.new_description.server_type}'."
        )
      end
    end
  end
end
