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

RSpec::Matchers.define :match_topology_opening_event do |expectation|

  match do |event|
    event.is_a?(Mongo::Monitoring::Event::TopologyOpening) &&
      event.topology != nil
  end
end

RSpec::Matchers.define :match_topology_description_changed_event do |expectation|
  include Mongo::SDAMMonitoring::Matchable

  match do |event|
    event.is_a?(Mongo::Monitoring::Event::TopologyChanged) &&
      topologies_match?(event, expectation)
  end
end

RSpec::Matchers.define :match_server_opening_event do |expectation|

  match do |event|
    event.is_a?(Mongo::Monitoring::Event::ServerOpening) &&
      event.address.to_s == expectation.data['address']
  end
end

RSpec::Matchers.define :match_server_description_changed_event do |expectation|
  include Mongo::SDAMMonitoring::Matchable

  match do |event|
    event.is_a?(Mongo::Monitoring::Event::ServerDescriptionChanged) &&
      descriptions_match?(event, expectation)
  end
end

RSpec::Matchers.define :match_server_closed_event do |expectation|

  match do |event|
    event.is_a?(Mongo::Monitoring::Event::ServerClosed) &&
      event.address.to_s == expectation.data['address']
  end
end

RSpec::Matchers.define :match_sdam_monitoring_event do |expectation|

  match do |event|
    expect(event).to send("match_#{expectation.name}", expectation)
  end
end

module Mongo
  module SDAMMonitoring
    module Matchable

      def descriptions_match?(event, expectation)
        description_matches?(event.previous_description, expectation.data['previousDescription']) &&
          description_matches?(event.new_description, expectation.data['newDescription'])
      end

      def topologies_match?(event, expectation)
        unless topology_matches?(event.previous_topology, expectation.data['previousDescription'])
          if ENV['VERBOSE_MATCHERS']
            $stderr.puts "Previous topology mismatch"
          end
          return false
        end
        unless topology_matches?(event.new_topology, expectation.data['newDescription'])
          if ENV['VERBOSE_MATCHERS']
            $stderr.puts "New topology mismatch:\nHave: #{event.new_topology}\nWant: #{expectation.data['newDescription']}"
          end
          return false
        end
        true
      end

      def description_matches?(actual, expected)
        type_ok = case expected['type']
          when 'Standalone' then actual.standalone?
          when 'RSPrimary' then actual.primary?
          when 'RSSecondary' then actual.secondary?
          when 'RSArbiter' then actual.arbiter?
          when 'Mongos' then actual.mongos?
          when 'Unknown' then actual.unknown?
          when 'PossiblePrimary' then actual.unknown?
          when 'RSGhost' then actual.ghost?
          when 'RSOther' then actual.other?
        end
        return false unless type_ok

        return false if actual.address.to_s != expected['address']
        return false if actual.arbiters != expected['arbiters']
        return false if actual.hosts != expected['hosts']
        return false if actual.passives != expected['passives']
        return false if actual.primary_host != expected['primary']
        return false if actual.replica_set_name != expected['setName']
        true
      end

      def topology_matches?(actual, expected)
        expected_type = ::Mongo::Cluster::Topology.const_get(expected['topologyType'])
        return false unless actual.is_a?(expected_type)

        return false unless actual.replica_set_name == expected['setName']

        expected['servers'].each do |server|
          desc = actual.server_descriptions[server['address'].to_s]
          return false unless description_matches?(desc, server)
        end

        actual.server_descriptions.keys.each do |address_str|
          unless expected['servers'].any? { |server| server['address'] == address_str }
            return false
          end
        end

        true
      end
    end

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
