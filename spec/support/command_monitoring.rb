# Copyright (C) 2014-2017 MongoDB, Inc.
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

RSpec::Matchers.define :match_command_name do |expectation|

  match do |event|
    expect(event.command_name.to_s).to eq(expectation.command_name.to_s)
  end
end

RSpec::Matchers.define :match_database_name do |expectation|

  match do |event|
    expect(event.database_name.to_s).to eq(expectation.database_name.to_s)
  end
end

RSpec::Matchers.define :generate_request_id do |expectation|

  match do |event|
    expect(event.request_id).to be > 0
  end
end

RSpec::Matchers.define :generate_operation_id do |expectation|

  match do |event|
    expect(event.request_id).to be > 0
  end
end

RSpec::Matchers.define :match_command do |expectation|
  include Mongo::CommandMonitoring::Matchable

  match do |event|
    data_matches?(event.command, expectation.event_data['command'])
  end
end

RSpec::Matchers.define :match_reply do |expectation|
  include Mongo::CommandMonitoring::Matchable

  match do |event|
    data_matches?(event.reply, expectation.event_data['reply'])
  end
end

RSpec::Matchers.define :match_command_started_event do |expectation|

  match do |event|
    expect(event).to match_command_name(expectation)
    expect(event).to match_database_name(expectation)
    expect(event).to generate_operation_id
    expect(event).to generate_request_id
    expect(event).to match_command(expectation)
  end
end

RSpec::Matchers.define :match_command_succeeded_event do |expectation|

  match do |event|
    expect(event).to match_command_name(expectation)
    expect(event).to generate_operation_id
    expect(event).to generate_request_id
    expect(event).to match_reply(expectation)
  end
end

RSpec::Matchers.define :match_command_failed_event do |expectation|

  match do |event|
    expect(event).to match_command_name(expectation)
    expect(event).to generate_operation_id
    expect(event).to generate_request_id
  end
end

module Mongo
  module CommandMonitoring

    # Matchers common behaviour.
    #
    # @since 2.1.0
    module Matchable

      # Determine if the data matches.
      #
      # @example Does the data match?
      #   matchable.data_matches?(actual, expected)
      #
      # @param [ Object ] actual The actual data.
      # @param [ Object ] expected The expected data.
      #
      # @return [ true, false ] If the data matches.
      #
      # @since 2.1.0
      def data_matches?(actual, expected)
        case expected
        when ::Hash, BSON::Document then
          hash_matches?(actual, expected)
        when ::Array
          array_matches?(actual, expected)
        else
          value_matches?(actual, expected)
        end
      end

      # Determine if the hash matches.
      #
      # @example Does the hash match?
      #   matchable.hash_matches?(actual, expected)
      #
      # @param [ Hash ] actual The actual hash.
      # @param [ Hash ] expected The expected hash.
      #
      # @return [ true, false ] If the hash matches.
      #
      # @since 2.1.0
      def hash_matches?(actual, expected)
        if expected['writeConcern']
          expected['writeConcern'] = Options::Mapper.transform_keys_to_symbols(expected['writeConcern'])
        end
        if expected.keys.first == '$numberLong'
          converted = expected.values.first.to_i
          (actual == converted) || actual >= 0
        else
          expected.each do |key, value|
            return false unless data_matches?(actual[key], value)
          end
        end
      end

      # Determine if an array matches.
      #
      # @example Does the array match?
      #   matchable.array_matches?(actual, expected)
      #
      # @param [ Array ] actual The actual array.
      # @param [ Array ] expected The expected array.
      #
      # @return [ true, false ] If the array matches.
      #
      # @since 2.1.0
      def array_matches?(actual, expected)
        expected.each_with_index do |value, i|
          # @todo: Durran: fix for kill cursors replies
          if actual
            return false unless data_matches?(actual[i], value)
          end
        end
      end

      # Check if a value matches.
      #
      # @example Does a value match.
      #   matchable.value_matches?(actual, expected)
      #
      # @param [ Object ] actual The actual value.
      # @param [ Object ] expected The expected object.
      #
      # @return [ true, false ] If the value matches.
      #
      # @since 2.1.0
      def value_matches?(actual, expected)
        case expected
        when '42', 42 then
          actual > 0
        when '' then
          !actual.nil?
        else
          actual == expected
        end
      end
    end

    # Represents a command monitoring spec in its entirety.
    #
    # @since 2.1.0
    class Spec

      # Create the spec.
      #
      # @example Create the spec.
      #   Spec.new('/path/to/test')
      #
      # @param [ String ] file The yaml test file.
      #
      # @since 2.1.0
      def initialize(file)
        file = File.new(file)
        @spec = YAML.load(ERB.new(file.read).result)
        file.close
        @data = @spec['data']
        @tests = @spec['tests']
      end

      # Get all the tests in the spec.
      #
      # @example Get all the tests.
      #   spec.tests
      #
      # @return [ Array<Test> ] The tests.
      def tests
        @tests.map do |test|
          Test.new(@data, test)
        end
      end
    end

    # Represents an individual command monitoring test.
    #
    # @since 2.1.0
    class Test

      # @return [ String ] description The test description.
      attr_reader :description

      # @return [ Array<Expectation> ] The expectations.
      attr_reader :expectations

      # @return [ String ] The server version to ignore if greater.
      attr_reader :ignore_if_server_version_greater_than

      # @return [ String ] The server version to ignore if lower.
      attr_reader :ignore_if_server_version_less_than

      # Create the new test.
      #
      # @example Create the test.
      #   Test.new(data, test)
      #
      # @param [ Array<Hash> ] data The test data.
      # @param [ Hash ] The test itself.
      #
      # @since 2.1.0
      def initialize(data, test)
        @data = data
        @description = test['description']
        @ignore_if_server_version_greater_than = test['ignore_if_server_version_greater_than']
        @ignore_if_server_version_less_than = test['ignore_if_server_version_less_than']
        @operation = Mongo::CRUD::Operation.get(test['operation'])
        @expectations = test['expectations'].map{ |e| Expectation.new(e) }
      end

      # Run the test against the provided collection.
      #
      # @example Run the test.
      #   test.run(collection)
      #
      # @param [ Mongo::Collection ] collection The collection.
      #
      # @since 2.1.0
      def run(collection)
        collection.insert_many(@data)
        @operation.execute(collection)
      end
    end

    # Encapsulates expectation behaviour.
    #
    # @since 2.1.0
    class Expectation

      # @return [ String ] event_type The type of expected event.
      attr_reader :event_type

      # @return [ Hash ] event_data The event data.
      attr_reader :event_data

      # Get the expected command name.
      #
      # @example Get the expected command name.
      #   expectation.command_name
      #
      # @return [ String ] The command name.
      #
      # @since 2.1.0
      def command_name
        @event_data['command_name']
      end

      # Get the expected database name.
      #
      # @example Get the expected database name.
      #   expectation.database_name
      #
      # @return [ String ] The database name.
      #
      # @since 2.1.0
      def database_name
        @event_data['database_name']
      end

      # Get a readable event name.
      #
      # @example Get the event name.
      #   expectation.event_name
      #
      # @return [ String ] The event name.
      #
      # @since 2.1.0
      def event_name
        event_type.gsub('_', ' ')
      end

      # Create the new expectation.
      #
      # @example Create the new expectation.
      #   Expectation.new(expectation)
      #
      # @param [ Hash ] expectation The expectation.
      #
      # @since 2.1.0
      def initialize(expectation)
        @event_type = expectation.keys.first
        @event_data = expectation[@event_type]
      end

      # Get the name of the matcher.
      #
      # @example Get the matcher name.
      #   expectation.matcher
      #
      # @return [ String ] The matcher name.
      #
      # @since 2.1.0
      def matcher
        "match_#{event_type}"
      end
    end

    # The test subscriber to track the events.
    #
    # @since 2.1.0
    class TestSubscriber

      def started(event)
        command_started_event[event.command_name] = event
      end

      def succeeded(event)
        command_succeeded_event[event.command_name] = event
      end

      def failed(event)
        command_failed_event[event.command_name] = event
      end

      private

      def command_started_event
        @started_events ||= BSON::Document.new
      end

      def command_succeeded_event
        @succeeded_events ||= BSON::Document.new
      end

      def command_failed_event
        @failed_events ||= BSON::Document.new
      end
    end
  end
end
