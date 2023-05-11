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

    # Single-doc tests focus on single-document read and write operations.
    # They are designed to give insights into the efficiency of the driver's
    # implementation of the basic wire protocol.
    #
    # @since 2.2.3
    module SingleDoc

      extend self

      # Run a Single Document benchmark test.
      #
      # @example Run a test.
      #   Benchmarking::SingleDoc.run(:command)
      #
      # @param [ Symbol ] type The type of test to run.
      # @param [ Integer ] repetitions The number of test repetitions.
      #
      # @return [ Numberic ] The test results.
      #
      # @since 2.2.3
      def run(type, repetitions = Benchmarking::TEST_REPETITIONS)
        Mongo::Logger.logger.level = ::Logger::WARN
        puts "#{type} : #{send(type, repetitions)}"
      end

      # Test sending a command to the server.
      #
      # @example Test sending an ismaster command.
      #   Benchmarking::SingleDoc.command(10)
      #
      # @param [ Integer ] repetitions The number of test repetitions.
      #
      # @since 2.2.3
      def command(repetitions)
        monitor = client.cluster.servers.first.monitor
        results = repetitions.times.collect do
          Benchmark.realtime do
            10_000.times do
              client.database.command(hello: true)
            end
          end
        end
        Benchmarking.median(results)
      end

      # Test sending find one by id.
      #
      # @example Test sending a find.
      #   Benchmarking::SingleDoc.find_one(10)
      #
      # @param [ Integer ] repetitions The number of test repetitions.
      #
      # @return [ Numeric ] The median of the results.
      #
      # @since 2.2.3
      def find_one(repetitions)
        client.database.drop
        doc = Benchmarking.tweet_document

        10_000.times do |i|
          doc[:_id] = i
          collection.insert_one(doc)
        end

        results = repetitions.times.collect do
          Benchmark.realtime do
            10_000.times do |i|
              collection.find({ _id: i }, limit: -1).first
            end
          end
        end
        Benchmarking.median(results)
      end

      # Test inserting a large document.
      #
      # @example Test inserting a large document.
      #   Benchmarking::SingleDoc.insert_one_large(10)
      #
      # @param [ Integer ] repetitions The number of test repetitions.
      #
      # @return [ Numeric ] The median of the results.
      #
      # @since 2.2.3
      def insert_one_large(repetitions)
        insert_one(repetitions, 10, Benchmarking.large_document)
      end

      # Test inserting a small document.
      #
      # @example Test inserting a small document.
      #   Benchmarking::SingleDoc.insert_one_small(10)
      #
      # @param [ Integer ] repetitions The number of test repetitions.
      #
      # @return [ Numeric ] The median of the results.
      #
      # @since 2.2.3
      def insert_one_small(repetitions)
        insert_one(repetitions, 10_000, Benchmarking.small_document)
      end

      private

      def insert_one(repetitions, do_repetitions, doc)
        client.database.drop
        create_collection

        results = repetitions.times.collect do
          Benchmark.realtime do
            do_repetitions.times do
              collection.insert_one(doc)
            end
          end
        end
        Benchmarking.median(results)
      end

      def client
        @client ||= Mongo::Client.new(["localhost:27017"], database: 'perftest', monitoring: false)
      end

      def collection
        @collection ||= begin; client[:corpus].tap { |coll| coll.create }; rescue Error::OperationFailure; client[:corpus]; end
      end
      alias :create_collection :collection
    end
  end
end
