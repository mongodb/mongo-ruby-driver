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
  module Monitoring

    # Defines a single event in a series for performance monitoring.
    #
    # @since 2.1.0
    class Event

      # @return [ String ] topic The event topic.
      attr_reader :topic

      # @return [ Float ] duration The duration of the event.
      attr_reader :duration

      # @return [ Hash ] payload The event payload.
      attr_reader :payload

      # Instantiate a new event.
      #
      # @example Instantiate the event.
      #   Event.new(Monitoring::QUERY, { filter: { name: 'test' }}, 0.10)
      #
      # @param [ String ] topic The event topic.
      # @param [ Hash ] payload The event payload.
      # @param [ Float ] duration The event duration.
      #
      # @since 2.1.0
      def initialize(topic, payload, duration)
        @topic = topic
        @payload = payload
        @duration = duration
      end
    end
  end
end
