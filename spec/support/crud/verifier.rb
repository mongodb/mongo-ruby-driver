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
      def verify_collection_data(actual_collection_data)
        expected_collection_data = test_instance.outcome_collection_data
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
      def verify_operation_result(actual)
        expected = test_instance.outcome['result']
        if expected.is_a?(Array)
          if expected.empty?
            expect(actual).to be_empty
          else
            expected.each_with_index do |expected_elt, i|
              verify_result(expected_elt, actual[i])
            end
          end
        else
          verify_result(expected, actual)
        end
      end

      private

      def verify_result(expected, actual)
        case expected
        when nil
          expect(actual).to be nil
        when Hash
          expected.each do |k, v|
            verify_hash_items_equal(expected, actual, k)
          end
        when Integer
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
        expect(expected[k]).to eq(actual[k])
      end
    end
  end
end
