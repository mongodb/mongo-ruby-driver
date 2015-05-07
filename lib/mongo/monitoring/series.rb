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

    # Defines behaviour around a series of events.
    #
    # @since 2.1.0
    class Series

      # @return [ String ] topic The series topic.
      attr_reader :topic

      # Return the total duration of all events.
      #
      # @example Return the series duration.
      #   series.duration
      #
      # @return [ Float ] The total duration.
      #
      # @since 2.1.0
      def duration
        events.reduce(0){ |sum, event| sum + event.duration }
      end

      # Get all the events in the series, in order.
      #
      # @example Get all the events.
      #   series.events
      #
      # @return [ Array<Event> ] The events in the series.
      #
      # @since 2.1.0
      def events
        @events ||= []
      end

      # Instantiate a new series.
      #
      # @example Create the new series.
      #   Series.new(Monitoring::QUERY)
      #
      # @param [ String ] topic The series topic.
      #
      # @since 2.1.0
      def initialize(topic)
        @topic = topic
      end

      # Push a new event to the series.
      #
      # @example Push an event to the series.
      #   series.push(event)
      #
      # @param [ Event ] event The next event in the series.
      #
      # @since 2.1.0
      def push(event)
        events.push(event)
      end
    end
  end
end
