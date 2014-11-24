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

require 'mongo/event/publisher'
require 'mongo/event/subscriber'
require 'mongo/event/server_added'
require 'mongo/event/server_removed'

module Mongo

  module Event

    # When a server is to be added to a cluster.
    #
    # @since 2.0.0
    SERVER_ADDED = 'server_added'.freeze

    # When a server is to be removed from a cluster.
    #
    # @since 2.0.0
    SERVER_REMOVED = 'server_removed'.freeze

    class << self

      # Add an event listener for the provided event.
      #
      # @example Add an event listener
      #   publisher.add_listener("my_event", listener)
      #
      # @param [ String ] event The event to listen for.
      # @param [ Object ] listener The event listener.
      #
      # @return [ Array<Object> ] The listeners for the event.
      #
      # @since 2.0.0
      def add_listener(event, listener)
        listeners_for(event).push(listener)
      end

      # Get all the event listeners.
      #
      # @example Get all the listeners.
      #   Event.listeners
      #
      # @return [ Hash<String, Array> ] The listeners.
      #
      # @since 2.0.0
      def listeners
        @listeners ||= {}
      end

      # Get the listeners for a specific event.
      #
      # @example Get the listeners.
      #   publisher.listeners_for("test")
      #
      # @param [ String ] event The event name.
      #
      # @return [ Array<Object> ] The listeners.
      #
      # @since 2.0.0
      def listeners_for(event)
        listeners[event] ||= []
      end
    end
  end
end
