# Copyright (C) 2019 MongoDB, Inc.
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

module Mongo
  module CRUD
    class Verifier
      include RSpec::Matchers

      def initialize(test_instance)
        @test_instance = test_instance
      end

      attr_reader :test_instance

      # Compare the existing collection data and the expected collection data.
      #
      # Uses RSpec matchers and raises expectation failures if there is a
      # mismatch.
      def verify_collection_data(expected_collection_data, actual_collection_data)
        if expected_collection_data.nil?
          expect(actual_collection_data).to be nil
        elsif expected_collection_data.empty?
          expect(actual_collection_data).to be_empty
        else
          expect(actual_collection_data).not_to be nil
          expected_collection_data.each do |doc|
            expect(actual_collection_data).to include(doc)
          end
          actual_collection_data.each do |doc|
            expect(expected_collection_data).to include(doc)
          end
        end
      end

      # Compare the actual operation result to the expected operation result.
      #
      # Uses RSpec matchers and raises expectation failures if there is a
      # mismatch.
      def verify_operation_result(expected, actual)
        if expected.is_a?(Array)
          if expected.empty?
            expect(actual).to be_empty
          else
            expected.each_with_index do |expected_elt, i|
              # If the YAML spec test does not define a result,
              # do not assert the operation's result - the operation may
              # have produced a result, the test just does not care what it is
              if expected_elt
                verify_result(expected_elt, actual[i])
              end
            end
          end
        else
          verify_result(expected, actual)
        end
      end

      def verify_command_started_event_count(expected_events, actual_events)
        expect(actual_events.length).to eq(expected_events.length)
      end

      def verify_command_started_event(expected_events, actual_events, i)
        expect(expected_events.length).to be > i
        expect(actual_events.length).to be > i

        expectation = expected_events[i]
        actual_event = actual_events[i]['command_started_event'].dup

        expect(expectation.keys).to eq(%w(command_started_event))
        expected_event = expectation['command_started_event'].dup
        # Retryable reads tests' YAML assertions omit some of the keys
        # that are included in the actual command events.
        # Transactions and transactions API tests specify all keys
        # in YAML that are present in actual command events.
        actual_event.keys.each do |key|
          unless expected_event.key?(key)
            actual_event.delete(key)
          end
        end
        expect(actual_event).not_to be nil
        expect(actual_event.keys).to eq(expected_event.keys)

        expected_command = expected_event.delete('command')
        actual_command = actual_event.delete('command')

        # Hash#compact is ruby 2.4+
        expected_presence = expected_command.select { |k, v| !v.nil? }
        expected_absence = expected_command.select { |k, v| v.nil? }

        expected_presence.each do |k, v|
          expect(k => actual_command[k]).to eq(k => v)
        end

        expected_absence.each do |k, v|
          expect(actual_command).not_to have_key(k)
        end

        # this compares remaining fields in events after command is removed
        expect(actual_event).to eq(expected_event)
      end

      private

      def verify_result(expected, actual)
        case expected
        when nil
          expect(actual).to be nil
        when Hash
          expected.each do |k, v|
            case k
            when 'errorContains'
              expect(actual['errorContains']).to include(v)
            when 'errorLabelsContain'
              v.each do |label|
                expect(actual['errorLabels']).to include(label)
              end
            when 'errorLabelsOmit'
              v.each do |label|
                if actual['errorLabels']
                  expect(actual['errorLabels']).not_to include(label)
                end
              end
            else
              verify_hash_items_equal(expected, actual, k)
            end
          end
        else
          expect(actual).to eq(expected)
        end
      end

      def verify_hash_items_equal(expected, actual, k)
        expect(actual).to be_a(Hash)

        if expected[k] == actual[k]
          return
        end

        if %w(deletedCount matchedCount modifiedCount upsertedCount).include?(k)
          # Some tests assert that some of these counts are zero.
          # The driver may omit the respective key, which is fine.
          if expected[k] == 0
            expect([0, nil]).to include(actual[k])
            return
          end
        end

        if %w(insertedIds upsertedIds).include?(k)
          if expected[k] == {}
            # Like with the counts, allow a response to not specify the
            # ids in question if the expectation is for an empty id map.
            expect([nil, []]).to include(actual[k])
          else
            expect(actual[k]).to eq(expected[k].values)
          end
          return
        end

        # This should produce a meaningful error message,
        # even though we do not actually require that expected[k] == actual[k]
        expect({k => expected[k]}).to eq({k => actual[k]})
      end
    end
  end
end
