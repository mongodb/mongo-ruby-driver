# frozen_string_literal: true
# rubocop:todo all

# Matcher for determining if the server is of the expected type according to
# the test.
#
# @since 2.0.0
RSpec::Matchers.define :be_server_type do |expected|

  match do |actual|
    Mongo::SDAM.server_of_type?(actual, expected)
  end
end

# Matcher for determining if the cluster topology is the expected type.
#
# @since 2.0.0
RSpec::Matchers.define :be_topology do |expected|

  match do |actual|
    actual.topology.class.name.sub(/.*::/, '') == expected
  end
end

module Mongo
  module SDAM

    module UniversalMethods
      def server_of_type?(server, type)
        case type
          when 'Standalone' then server.standalone?
          when 'RSPrimary' then server.primary?
          when 'RSSecondary' then server.secondary?
          when 'RSArbiter' then server.arbiter?
          when 'Mongos' then server.mongos?
          when 'Unknown' then server.unknown?
          when 'PossiblePrimary' then server.unknown?
          when 'RSGhost' then server.ghost?
          when 'RSOther' then server.other?
          when 'LoadBalancer' then server.load_balancer?
          else
            raise "Unknown type #{type}"
        end
      end
    end

    include UniversalMethods
    extend UniversalMethods

    # Convenience helper to find a server by it's URI.
    #
    # @since 2.0.0
    def find_server(client, address_str)
      client.cluster.servers_list.detect{ |s| s.address.to_s == address_str }
    end

    # Represents a specification.
    #
    # @since 2.0.0
    class Spec

      # @return [ String ] description The spec description.
      attr_reader :description

      # @return [ Array<Phase> ] phases The spec phases.
      attr_reader :phases

      # @return [ Mongo::URI ] uri The URI object.
      attr_reader :uri

      # @return [ String ] uri_string The passed uri string.
      attr_reader :uri_string

      # Instantiate the new spec.
      #
      # @param [ String ] test_path The path to the file.
      #
      # @since 2.0.0
      def initialize(test_path)
        @test = ::Utils.load_spec_yaml_file(test_path)
        @description = @test['description']
        @uri_string = @test['uri']
        @uri = URI.new(uri_string)
        @phases = @test['phases'].map{ |phase| Phase.new(phase, uri) }
      end
    end

    # Represents a phase in the spec. Phases are sequential.
    #
    # @since 2.0.0
    class Phase

      # @return [ Outcome ] outcome The phase outcome.
      attr_reader :outcome

      # @return [ Array<Response> ] responses The responses for each server in
      #   the phase.
      attr_reader :responses

      attr_reader :application_errors

      # Create the new phase.
      #
      # @example Create the new phase.
      #   Phase.new(phase, uri)
      #
      # @param [ Hash ] phase The phase hash.
      # @param [ Mongo::URI ] uri The URI.
      #
      # @since 2.0.0
      def initialize(phase, uri)
        @phase = phase
        @responses = @phase['responses']&.map{ |response| Response.new(response, uri) }
        @application_errors = @phase['applicationErrors']&.map{ |error_spec| ApplicationError.new(error_spec) }
        @outcome = Outcome.new(BSON::ExtJSON.parse_obj(@phase['outcome']))
      end
    end

    # Represents a server response during a phase.
    #
    # @since 2.0.0
    class Response

      # @return [ String ] address The server address.
      attr_reader :address

      # @return [ Hash ] hello The hello response.
      attr_reader :hello

      # Create the new response.
      #
      # @example Create the response.
      #   Response.new(response, uri)
      #
      # @param [ Hash ] response The response value.
      # @param [ Mongo::URI ] uri The URI.
      #
      # @since 2.0.0
      def initialize(response, uri)
        @uri = uri
        @address = response[0]
        @hello = BSON::ExtJSON.parse_obj(response[1])
      end
    end

    class ApplicationError
      def initialize(spec)
        @spec = spec
      end

      def address_str
        @spec.fetch('address')
      end

      def when
        ::Utils.underscore(@spec.fetch('when'))
      end

      def max_wire_version
        @spec['max_wire_version']
      end

      def generation
        @spec['generation']
      end

      def type
        ::Utils.underscore(@spec.fetch('type'))
      end

      def result
        msg = Mongo::Protocol::Msg.new([], {}, BSON::ExtJSON.parse_obj(@spec['response']))
        Mongo::Operation::Result.new([msg])
      end
    end

    # Get the outcome or expectations from the phase.
    #
    # @since 2.0.0
    class Outcome

      # @return [ Array ] events The expected events.
      attr_reader :events

      # @return [ Hash ] servers The expecations for
      #   server states.
      attr_reader :servers

      # @return [ String ] set_name The expected RS set name.
      attr_reader :set_name

      # @return [ String ] topology_type The expected cluster topology type.
      attr_reader :topology_type

      # @return [ Integer, nil ] logical_session_timeout The expected logical session timeout.
      attr_reader :logical_session_timeout

      attr_reader :max_election_id

      attr_reader :max_set_version

      # Create the new outcome.
      #
      # @example Create the new outcome.
      #   Outcome.new(outcome)
      #
      # @param [ Hash ] outcome The outcome object.
      #
      # @since 2.0.0
      def initialize(outcome)
        @servers = outcome['servers'] if outcome['servers']
        @set_name = outcome['setName']
        @topology_type = outcome['topologyType']
        @logical_session_timeout = outcome['logicalSessionTimeoutMinutes']
        @events = map_events(outcome['events']) if outcome['events']
        @compatible = outcome['compatible']
        if outcome['maxElectionId']
          @max_election_id = outcome['maxElectionId']
        end
        @max_set_version = outcome['maxSetVersion']
      end

      # Whether the server responses indicate that their versions are supported by the driver.
      #
      # @example Do the server responses indicate that their versions are supported by the driver.
      #   outcome.compatible?
      #
      # @return [ true, false ] Whether the server versions are compatible with the driver.
      #
      # @since 2.5.1
      def compatible?
        @compatible.nil? || !!@compatible
      end

      def compatible_specified?
        !@compatible.nil?
      end

      private

      def map_events(events)
        events.map do |event|
          Event.new(event.keys.first, event.values.first)
        end
      end
    end

    class Event

      MAPPINGS = {
        'server_closed_event' => Mongo::Monitoring::Event::ServerClosed,
        'server_description_changed_event' => Mongo::Monitoring::Event::ServerDescriptionChanged,
        'server_opening_event' => Mongo::Monitoring::Event::ServerOpening,
        'topology_description_changed_event' => Mongo::Monitoring::Event::TopologyChanged,
        'topology_opening_event' => Mongo::Monitoring::Event::TopologyOpening
      }.freeze

      attr_reader :name
      attr_reader :data

      def initialize(name, data)
        @name = name
        @data = data
      end

      def expected
        MAPPINGS.fetch(name)
      end
    end
  end
end

class SdamSpecEventPublisher
  include Mongo::Event::Publisher

  def initialize(event_listeners)
    @event_listeners = event_listeners
  end
end
