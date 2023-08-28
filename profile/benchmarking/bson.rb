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

require_relative 'percentiles'
require_relative 'summary'

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

      # As defined by the spec, the score for a given benchmark is the
      # size of the task (in MB) divided by the median wall clock time.
      #
      # @param [ Symbol ] type the type of the task
      # @param [ Mongo::Benchmarking::Percentiles ] percentiles the Percentiles
      #   object to query for the median time.
      # @param [ Numeric ] scale the number of times the operation is performed
      #   per iteration, used to scale the task size.
      #
      # @return [ Numeric ] the score for the given task.
      def score_for(type, percentiles, scale: 10_000)
        task_size(type, scale) / percentiles[50]
      end

      # Run a BSON benchmark test.
      #
      # @example Run a test.
      #   Benchmarking::BSON.run(:flat)
      #
      # @param [ Symbol ] type The type of test to run.
      # @param [ :encode | :decode ] action The action to perform.
      #
      # @return [ Hash<:timings,:percentiles,:score> ] The test results for
      #    the requested benchmark.
      def run(type, action)
        timings = send(action, file_for(type))
        percentiles = Percentiles.new(timings)
        score = score_for(type, percentiles)

        Summary.new(timings, percentiles, score)
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

      private

      # The path to the source file for the given task type.
      #
      # @param [ Symbol ] type the task type
      #
      # @return [ String ] the path to the source file.
      def file_for(type)
        File.join(Benchmarking::DATA_PATH, "#{type}_bson.json")
      end

      # As defined by the spec, the size of a BSON task is the size of the
      # file, multipled by the scale (the number of times the file is processed
      # per iteration), divided by a million.
      #
      # "the dataset size for a task is the size of the single-document source
      # file...times 10,000 operations"
      #
      # "Each task will have defined for it an associated size in
      # megabytes (MB)"
      #
      # @param [ Symbol ] type the type of the task
      # @param [ Numeric ] scale the number of times the operation is performed
      #   per iteration (e.g. 10,000)
      #
      # @return [ Numeric ] the score for the task, reported in MB
      def task_size(type, scale)
        File.size(file_for(type)) * scale / 1_000_000.0
      end
    end
  end
end
