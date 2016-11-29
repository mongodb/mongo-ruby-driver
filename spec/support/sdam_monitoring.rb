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
    event.topology != nil
  end
end

RSpec::Matchers.define :match_topology_description_changed_event do |expectation|
  include Mongo::SDAMMonitoring::Matchable

  match do |event|
    topologies_match?(event, expectation)
  end
end

RSpec::Matchers.define :match_server_opening_event do |expectation|

  match do |event|
    true
  end
end

RSpec::Matchers.define :match_server_description_changed_event do |expectation|
  include Mongo::SDAMMonitoring::Matchable

  match do |event|
    descriptions_match?(event, expectation)
  end
end

RSpec::Matchers.define :match_server_closed_event do |expectation|

  match do |event|
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
        topology_matches?(event.previous_topology, expectation.data['previousDescription']) &&
          topology_matches?(event.new_topology, expectation.data['newDescription'])
      end

      def description_matches?(actual, expected)
        case expected['type']
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
      end

      def topology_matches?(actual, expected)
        case expected['topologyType']
          when 'ReplicaSetWithPrimary' then actual.replica_set?
          when 'ReplicaSetNoPrimary' then (actual.replica_set? || actual.unknown?)
          when 'Sharded' then actual.sharded?
          when 'Single' then actual.single?
          when 'Unknown' then actual.unknown?
        end
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
        matching = events.find do |event|
          event.class == MAPPINGS[name]
        end
        events.delete(matching)
        matching
      end

      private

      def events
        @events ||= []
      end
    end
  end
end
