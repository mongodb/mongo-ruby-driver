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

    # The command topic.
    #
    # @since 2.1.0
    COMMAND = 'Command'.freeze

    class << self

      def started(topic, event)
        subscribers_for(topic).each{ |subscriber| subscriber.started(event) }
      end

      def completed(topic, event)
        subscribers_for(topic).each{ |subscriber| subscriber.completed(event) }
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

      def subscribers?(topic)
        !subscribers_for(topic).empty?
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
