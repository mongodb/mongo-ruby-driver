# Copyright (C) 2014-2015 MongoDB, Inc.
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
#

module Mongo
  module SDAMMonitoring

    # Test subscriber for SDAM monitoring.
    #
    # @since 2.4.0
    class TestSubscriber

      # The mappings of event names to types.
      #
      # @since 2.4.0
      MAPPINGS = {
        'topology_opening_event' => Mongo::Monitoring::Event::TopologyOpening,
        'topology_description_changed_event' => Mongo::Monitoring::Event::TopologyChanged,
        'topology_closed_event' => Mongo::Monitoring::Event::TopologyClosed,
        'server_opening_event' => Mongo::Monitoring::Event::ServerOpening,
        'server_description_changed_event' => Mongo::Monitoring::Event::ServerDescriptionChanged,
        'server_closed_event' => Mongo::Monitoring::Event::ServerClosed
      }.freeze

      # Implement the succeeded event.
      #
      # @param [ Event ] event The event.
      #
      # @since 2.4.0
      def succeeded(event)
        events.push(event)
      end

      # Get the first event fired for the name, and then delete it.
      #
      # @param [ String ] name The event name.
      #
      # @return [ Event ] The matching event.
      def first_event(name)
        cls = MAPPINGS[name]
        if cls.nil?
          raise ArgumentError, "Bogus event name #{name}"
        end
        matching = events.find do |event|
          cls === event
        end
        events.delete(matching)
        matching
      end

      def events
        @events ||= []
      end
    end

    class PhasedTestSubscriber < TestSubscriber
      def initialize
        super
        @phase_events = {}
      end

      def phase_finished(phase_index)
        @phase_events[phase_index] = events
        @events = []
      end

      def phase_events(phase_index)
        @phase_events[phase_index]
      end

      def event_count
        @phase_events.inject(0) do |sum, event|
          sum + event.length
        end
      end
    end
  end
end
