# frozen_string_literal: true

# Copyright (C) 2015-2020 MongoDB Inc.
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
  module Benchmarking
    # These tests focus on BSON encoding and decoding; they are client-side only and
    # do not involve any transmission of data to or from the server.
    module BSON
      extend self

      # Runs all of the benchmarks specified by the given mapping.
      #
      # @example Run a collection of benchmarks.
      #   Benchmarking::BSON.run_all(
      #     flat: %i[ encode decode ],
      #     deep: %i[ encode decode ],
      #     full: %i[ encode decode ]
      #   )
      #
      # @return [ Hash ] a hash of the results for each benchmark
      def run_all(map)
        {}.tap do |results|
          map.each do |type, actions|
            results[type] = {}

            actions.each do |action|
              results[type][action] = run(type, action)
            end
          end
        end
      end

      # Run a BSON benchmark test.
      #
      # @example Run a test.
      #   Benchmarking::BSON.run(:flat)
      #
      # @param [ Symbol ] type The type of test to run.
      # @param [ :encode | :decode ] action The action to perform.
      #
      # @return [ Array<Number> ] The test results for each iteration
      def run(type, action)
        file_path = File.join(Benchmarking::DATA_PATH, "#{type}_bson.json")
        Benchmarking.without_gc { send(action, file_path) }
      end

      # Run an encoding BSON benchmark test.
      #
      # @example Run an encoding test.
      #   Benchmarking::BSON.encode(file_name)
      #
      # @param [ String ] file_name The name of the file with data for the test.
      # @param [ Integer ] repetitions The number of test repetitions.
      #
      # @return [ Array<Numeric> ] The list of the results for each iteration
      def encode(file_name)
        data = Benchmarking.load_file(file_name)
        document = ::BSON::Document.new(data.first)

        Benchmarking.benchmark do
          10_000.times { document.to_bson }
        end
      end

      # Run a decoding BSON benchmark test.
      #
      # @example Run an decoding test.
      #   Benchmarking::BSON.decode(file_name)
      #
      # @param [ String ] file_name The name of the file with data for the test.
      # @param [ Integer ] repetitions The number of test repetitions.
      #
      # @return [ Array<Numeric> ] The list of the results for each iteration
      def decode(file_name)
        data = Benchmarking.load_file(file_name)
        buffer = ::BSON::Document.new(data.first).to_bson

        Benchmarking.benchmark do
          10_000.times do
            ::BSON::Document.from_bson(buffer)
            buffer.rewind!
          end
        end
      end
    end
  end
end
