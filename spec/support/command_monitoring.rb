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

    class Spec


      def initialize(file)
        @spec = YAML.load(ERB.new(File.new(file).read).result)
        @data = @spec['data']
        @tests = @spec['tests']
      end

      def tests
        @tests.map do |test|
          Test.new(@data, test)
        end
      end
    end

    class Test

      attr_reader :description

      attr_reader :expectations

      def initialize(data, test)
        @data = data
        @description = test['description']
        @operation = Mongo::CRUD::Operation.get(test['operation'])
        @expectations = test['expectations'].map{ |e| Expectation.new(e) }
      end

      def run(collection)
        collection.insert_many(@data)
        @operation.execute(collection)
      end
    end

    class Expectation

      attr_reader :event_type

      def command_name
        @event_data['command_name']
      end

      def database_name
        @event_data['database_name']
      end

      def event_name
        event_type.gsub('_', ' ')
      end

      def initialize(expectation)
        @event_type = expectation.keys.first
        @event_data = expectation[@event_type]
      end

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

      def matches_started_event?(event)
        event.command_name.to_s == command_name &&
          event.database_name.to_s == database_name &&
          matches_command?(event)
      end

      def matches_succeeded_event?(event)
        event.command_name.to_s == command_name &&
          event.database_name.to_s == database_name
      end

      def matches_failed_event?(event)
        event.command_name.to_s == command_name &&
          event.database_name.to_s == database_name
      end

      def matches_command?(event)
        @event_data['command'].each do |key, value|
          return false if event.command[key] != value
        end
        true
      end
    end

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
