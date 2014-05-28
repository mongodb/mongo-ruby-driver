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

    # This module is included for objects that need to publish events.
    #
    # @since 2.0.0
    module Publisher

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

      # Publish the provided event.
      #
      # @example Publish an event.
      #   publisher.publish("my_event", "payload")
      #
      # @param [ String ] event The event to publish.
      # @param [ Array<Object> ] args The objects to pass to the listeners.
      #
      # @since 2.0.0
      def publish(event, *args)
        listeners_for(event).each { |listener| listener.handle(*args) }
      end

      # Get all the listeners for the publisher.
      #
      # @example Get all the listeners.
      #   publisher.listeners
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
