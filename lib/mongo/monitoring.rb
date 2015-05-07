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

require 'mongo/monitoring/event'
require 'mongo/monitoring/publishable'

module Mongo

  # The module defines behaviour for the performance monitoring API.
  #
  # @since 2.1.0
  module Monitoring

    # The query event constant.
    #
    # @since 2.1.0
    QUERY = 'Query'.freeze

    # The get more event constant.
    #
    # @since 2.1.0
    GET_MORE = 'Get More'.freeze

    # The kill cursors event constant.
    #
    # @since 2.1.0
    KILL_CURSORS = 'Kill Cursors'.freeze

    class << self

      # Publish a global series of events.
      #
      # @example Publish the event.
      #   Monitoring.publish(QUERY, event)
      #
      # @param [ String ] topic The event topic.
      # @param [ Event ] event The event.
      #
      # @since 2.1.0
      def publish(topic, event)
        subscribers_for(topic).each{ |subscriber| subscriber.notify(event) }
      end

      # Subscribe a listener to an event topic.
      #
      # @example Subscribe to the topic.
      #   Monitoring.subscribe(QUERY, subscriber)
      #
      # @param [ String ] topic The event topic.
      # @param [ Object ] subscriber The subscriber to handle the event.
      #
      # @since 2.1.0
      def subscribe(topic, subscriber)
        subscribers_for(topic).push(subscriber)
      end

      private

      def subscribers
        @subscribers ||= {}
      end

      def subscribers_for(topic)
        subscribers[topic] ||= []
      end
    end
  end
end
