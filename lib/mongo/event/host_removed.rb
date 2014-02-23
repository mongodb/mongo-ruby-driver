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
  module Event

    # This handles host removed events for server descriptions.
    #
    # @since 3.0.0
    class HostRemoved
      include Loggable

      # @return [ Mongo::Server ] server The event publisher.
      attr_reader :server

      # Initialize the new host removed event handler.
      #
      # @example Create the new handler.
      #   HostRemoved.new(server)
      #
      # @param [ Mongo::Server ] server The server to publish from.
      #
      # @since 3.0.0
      def initialize(server)
        @server = server
      end

      # This event publishes an event to remove the server and logs the
      # configuration change.
      #
      # @example Handle the event.
      #   host_removed.handle('127.0.0.1:27018')
      #
      # @param [ String ] address The removed host.
      #
      # @since 3.0.0
      def handle(address)
        log(:debug, 'MONGODB', [ "#{address} removed from replica set configuration." ])
        server.publish(Event::SERVER_REMOVED, address)
      end
    end
  end
end
