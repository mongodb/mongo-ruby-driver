# frozen_string_literal: true
# rubocop:todo all

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
    #
    # @since 2.2.3
    module Micro

      extend self

      # Run a micro benchmark test.
      #
      # @example Run a test.
      #   Benchmarking::Micro.run(:flat)
      #
      # @param [ Symbol ] type The type of test to run.
      # @param [ Integer ] repetitions The number of test repetitions.
      #
      # @return [ Numeric ] The test results.
      #
      # @since 2.2.3
      def run(type, action, repetitions = Benchmarking::TEST_REPETITIONS)
        file_name = type.to_s << "_bson.json"
        GC.disable
        file_path = [Benchmarking::DATA_PATH, file_name].join('/')
        puts "#{action} : #{send(action, file_path, repetitions)}"
      end

      # Run an encoding micro benchmark test.
      #
      # @example Run an encoding test.
      #   Benchmarking::Micro.encode(file_name)
      #
      # @param [ String ] file_name The name of the file with data for the test.
      # @param [ Integer ] repetitions The number of test repetitions.
      #
      # @return [ Numeric ] The median of the results.
      #
      # @since 2.2.3
      def encode(file_name, repetitions)
        data = Benchmarking.load_file(file_name)
        document = BSON::Document.new(data.first)

        # WARMUP_REPETITIONS.times do
        #   doc.to_bson
        # end

        results = repetitions.times.collect do
          Benchmark.realtime do
            10_000.times do
              document.to_bson
            end
          end
        end
        Benchmarking.median(results)
      end

      # Run a decoding micro benchmark test.
      #
      # @example Run an decoding test.
      #   Benchmarking::Micro.decode(file_name)
      #
      # @param [ String ] file_name The name of the file with data for the test.
      # @param [ Integer ] repetitions The number of test repetitions.
      #
      # @return [ Numeric ] The median of the results.
      #
      # @since 2.2.3
      def decode(file_name, repetitions)
        data = Benchmarking.load_file(file_name)
        buffer = BSON::Document.new(data.first).to_bson

        # WARMUP_REPETITIONS.times do
        #   BSON::Document.from_bson(buffers.shift)
        # end

        results = repetitions.times.collect do
          Benchmark.realtime do
            10_000.times do
              BSON::Document.from_bson(buffer)
              buffer.rewind!
            end
          end
        end
        Benchmarking.median(results)
      end
    end
  end
end
