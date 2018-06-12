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

  failure_message do |event|
    diff_topologies(event, expectation)
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

      def diff_topologies(event, expectation)
        msg = []

        unless topology_matches?(event.previous_topology, expectation.data['previousDescription'])
          msg << "Previous topologies mismatch: expected: #{inspect_spec_topology(expectation.data['previousDescription'])}, actual: #{inspect_ruby_topology(event.previous_topology)}"
        end

        unless topology_matches?(event.new_topology, expectation.data['newDescription'])
          msg << "New topologies mismatch: expected: #{inspect_spec_topology(expectation.data['newDescription'])}, actual: #{inspect_ruby_topology(event.new_topology)}"
        end

        msg = "Topologies mismatch: #{msg.join(', ')}"
        # HACK: Returning the message doesn't seem to do anything,
        # print it out for now
        puts msg
        msg
      end

      def inspect_ruby_topology(actual)
        "type=#{ruby_topology_type(actual)}"
      end

      def inspect_spec_topology(expected)
        "type=#{spec_topology_type(expected)}"
      end

      def ruby_description(actual)
        type = if actual.standalone?
          'Standalone'
        elsif actual.primary?
          'RSPrimary'
        elsif actual.secondary?
          'RSSecondary'
        elsif actual.arbiter?
          'RSArbiter'
        elsif actual.mongos?
          'Mongos'
        elsif actual.unknown?
          'Unknown/PossiblePrimary'
        elsif actual.ghost?
          'RSGhost'
        elsif actual.other?
          'RSOther'
        else
          'Unhandled'
        end
      end

      def spec_description(expected)
        case expected['type']
        when 'Unknown'
          'Unknown/PossiblePrimary'
        when 'PossiblePrimary'
          'Unknown/PossiblePrimary'
        else
          expected['type']
        end
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

      def ruby_topology_type(actual)
        if actual.replica_set?
          'Replica Set'
        elsif actual.sharded?
          'Sharded'
        elsif actual.single?
          'Single'
        elsif actual.unknown?
          'Unknown'
        else
          'Unhandled'
        end
      end

      def spec_topology_type(expected)
        case expected['topologyType']
        when 'ReplicaSetWithPrimary'
          'Replica Set'
        when 'ReplicaSetNoPrimary'
          'Replica Set/Unknown'
        when 'Sharded'
          'Sharded'
        when 'Single'
          'Single'
        when 'Unknown'
          'Unknown'
        else
          'Unhandled'
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
  #require 'byebug';byebug
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
