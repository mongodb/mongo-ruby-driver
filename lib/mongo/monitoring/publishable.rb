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

    # Defines behaviour for an object that can publish monitoring events.
    #
    # @since 2.1.0
    module Publishable

      # Publish an event series to the global monitoring.
      #
      # @example Publish the event series.
      #   object.publish(Monitoring::QUERY, series)
      #
      # @param [ String ] topic The event series topic.
      # @param [ Series ] series The series of events.
      #
      # @since 2.1.0
      def publish(topic, series)
        Monitoring.publish(topic, series)
      end

      # Record an event in the series.
      #
      # @example Record an event.
      #   object.record(series, Monitoring::QUERY, { filter: { name: 'test' }})
      #
      # @param [ Series ] series The series of events.
      # @param [ String ] topic The event topic.
      # @param [ Hash ] payload The event payload.
      #
      # @return [ Object ] The result of the provided block.
      #
      # @since 2.1.0
      def record(series, topic, payload)
        start = Time.now
        result = yield
        duration = Time.now - start
        series.push(Monitoring::Event.new(topic, payload, duration))
        result
      end
    end
  end
end
