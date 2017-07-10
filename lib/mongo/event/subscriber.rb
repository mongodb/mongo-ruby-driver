# Copyright (C) 2014-2017 MongoDB, Inc.
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

    # Adds convenience methods for adding listeners to event publishers.
    #
    # @since 2.0.0
    module Subscriber

      # @return [ Event::Listeners ] event_listeners The listeners.
      attr_reader :event_listeners

      # Subscribe to the provided event.
      #
      # @example Subscribe to the event.
      #   subscriber.subscribe_to('test', listener)
      #
      # @param [ String ] event The event.
      # @param [ Object ] listener The event listener.
      #
      # @since 2.0.0
      def subscribe_to(event, listener)
        event_listeners.add_listener(event, listener)
      end
    end
  end
end

