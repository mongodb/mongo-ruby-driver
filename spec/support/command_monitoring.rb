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

RSpec::Matchers.define :match_expected_event do |expectation|

  match do |event|
    expectation.matches?(event)
  end
end

module Mongo
  module CommandMonitoring

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
        @spec = YAML.load(ERB.new(File.new(file).read).result)
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

      # Determine if the event matches the expectation.
      #
      # @example Does the event match the expectation?
      #   expecation.matches?(event)
      #
      # @param [ Event ] The monitoring event.
      #
      # @return [ true, false ] If they match.
      #
      # @since 2.1.0
      def matches?(event)
        case event_type
        when 'command_started_event'
          matches_started_event?(event)
        when 'command_succeeded_event'
          matches_succeeded_event?(event)
        when 'command_failed_event'
          matches_failed_event?(event)
        end
      end

      private

      def matches_started_event?(event)
        matches_common_attributes?(event) && matches_command?(event)
      end

      def matches_succeeded_event?(event)
        matches_common_attributes?(event) && matches_reply?(event)
      end

      def matches_failed_event?(event)
        matches_common_attributes?(event)
      end

      def matches_common_attributes?(event)
        event.command_name.to_s == command_name &&
          event.database_name.to_s == database_name &&
          event.operation_id >= 0 &&
          event.request_id >= 0
      end

      def matches_command?(event)
        @event_data['command'].each do |key, value|
          if key == 'writeConcern'
            return false if event.command[key] != BSON::Document.new(WRITE_CONCERN)
          end
          return false if event.command[key] != value
        end
        case event.command_name
        when 'getMore'
          return false if event.command['getMore'] <= 0
        when 'killCursors'
          return false if event.command['cursors'].first <= 0
        end
        true
      end

      def matches_reply?(event)
        @event_data['reply'].each do |key, value|
          if key == 'cursor'
            return false unless matches_cursor?(event.reply[key], value)
          else
            return false if event.reply[key] != value
          end
        end
        true
      end

      def matches_cursor?(event_cursor, cursor)
        cursor.each do |key, value|
          return false if event_cursor[key] != value
        end
        true
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
