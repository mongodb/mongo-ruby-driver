# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2014-2020 MongoDB Inc.
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

      # @return [ Event::Listeners ] event_listeners The listeners.
      attr_reader :event_listeners

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
        event_listeners.listeners_for(event).each do |listener|
          listener.handle(*args)
        end
      end
    end
  end
end
