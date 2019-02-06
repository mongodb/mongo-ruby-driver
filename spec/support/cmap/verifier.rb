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
  module Cmap
    class Verifier
      include RSpec::Matchers

      def initialize(test_instance)
        @test_instance = test_instance
      end

      attr_reader :test_instance

      # Compare the existing CMAP events data and the expected CMAP events.
      #
      # Uses RSpec matchers and raises expectation failures if there is a
      # mismatch.
      def verify_events(actual_events)
        expected_events = test_instance.events
        if expected_events.nil?
          expect(actual_events).to be nil
        elsif expected_events.empty?
          expect(actual_events).to be_empty
        else
          expect(actual_events).not_to be nil
          expected_events.each do |event|
            verify_event(event, actual_events)
          end
          actual_events.each do |event|
            verify_event(event, expected_events)
          end
        end
      end

      private

      def verify_event(event, events)
        expect(
          events.any? { |other| verify_hashes(event, other) }
        ).to eq(true)
      end

      def verify_hashes(expected, actual)
        return false unless expected.size == actual.size

        expected.all? do |key, val|
          return false unless actual.key?(key)

          if val == 42 || actual[key] == 42
            true
          elsif val.is_a?(Hash)
            verify_hashes(val, actual[key])
          else
            val == actual[key]
          end
        end
      end
    end
  end
end
