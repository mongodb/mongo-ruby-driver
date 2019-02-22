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
  module Transactions
    class Verifier < CRUD::Verifier

      def verify_command_started_event_count(results)
        expectations = test_instance.expectations
        expect(results[:events].length).to eq(expectations.length)
      end

      def verify_command_started_event(results, i)
        expectation = test_instance.expectations[i]

        expect(expectation.keys).to eq(%w(command_started_event))
        expected_event = expectation['command_started_event'].dup
        actual_event = results[:events][i].dup
        expect(actual_event).not_to be nil
        expect(expected_event.keys).to eq(actual_event.keys)

        expected_command = expected_event.delete('command')
        actual_command = actual_event.delete('command')

        # Hash#compact is ruby 2.4+
        expected_presence = expected_command.select { |k, v| !v.nil? }
        expected_absence = expected_command.select { |k, v| v.nil? }

        expect(actual_command).to eq(expected_presence)
        expected_absence.each do |k, v|
          expect(actual_command).not_to have_key(k)
        end

        # this compares remaining fields in events after command is removed
        expect(expected_event).to eq(actual_event)
      end

      def verify_operation_result(actual_results)
        expected_results = test_instance.expected_results

        expect(actual_results.length).to eq(expected_results.length)

        index = 0
        expected_results.zip(actual_results).each do |expected, actual|
          if expected
            verify_result(expected, actual)
          end
        end
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
              expect(actual).to be_a(Hash)
              ok = (actual[k] == v || handle_upserted_id(k, v, actual[v]) ||
                handle_inserted_ids(k, v, actual[v]))
              expect(ok).to be true
            end
          end
        else
          expect(actual).to eq(expected)
        end
      end

    end
  end
end
