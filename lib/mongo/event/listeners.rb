# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2015-2020 MongoDB Inc.
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
  module Event

    # The queue of events getting processed in the client.
    #
    # @since 2.0.0
    class Listeners

      # Initialize the event listeners.
      #
      # @example Initialize the event listeners.
      #   Listeners.new
      #
      # @since 2.0.0
      def initialize
        @listeners = {}
      end

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
        @listeners[event] ||= []
      end
    end
  end
end
