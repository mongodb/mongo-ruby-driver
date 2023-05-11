# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2019-2020 MongoDB Inc.
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

      def verify_hashes(actual, expected)
        expect(expected).to be_a(Hash)
        expect(actual).to be_a(Hash)

        actual_modified = actual.dup
        if actual['reason']
          actual_modified['reason'] = actual['reason'].to_s.gsub(/_[a-z]/) { |m| m[1].upcase }
        end

        actual.each do |k, v|
          if expected.key?(k) && expected[k] == 42 && v
            actual_modified[k] = 42
          end
        end

        expect(actual_modified.slice(*expected.keys)).to eq(expected)
      end
    end
  end
end
