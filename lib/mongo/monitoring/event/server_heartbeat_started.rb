# Copyright (C) 2018-2019 MongoDB, Inc.
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

      # Event fired when a server heartbeat is dispatched.
      #
      # @since 2.7.0
      class ServerHeartbeatStarted < Mongo::Event::Base

        # @return [ Address ] address The server address.
        attr_reader :address

        # Create the event.
        #
        # @example Create the event.
        #   ServerHeartbeatStarted.new(address)
        #
        # @param [ Address ] address The server address.
        #
        # @since 2.7.0
        # @api private
        def initialize(address)
          @address = address
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
          "#<#{self.class.name.sub(/^Mongo::Monitoring::Event::/, '')}" +
          " address=#{address}>"
        end
      end
    end
  end
end
