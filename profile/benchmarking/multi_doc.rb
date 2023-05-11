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

    # Multi-doc benchmarks focus on multiple-document read and write operations.
    # They are designed to give insight into the efficiency of the driver's implementation
    # of bulk/batch operations such as bulk writes and cursor reads.
    #
    # @since 2.2.3
    module MultiDoc

      extend self

      # Run a multi-document benchmark test.
      #
      # @example Run a test.
      #   Benchmarking::MultiDoc.run(:find_many)
      #
      # @param [ Symbol ] type The type of test to run.
      # @param [ Integer ] repetitions The number of test repetitions.
      #
      # @return [ Numeric ] The test results.
      #
      # @since 2.2.3
      def run(type, repetitions = Benchmarking::TEST_REPETITIONS)
        Mongo::Logger.logger.level = ::Logger::WARN
        puts "#{type} : #{send(type, repetitions)}"
      end

      # Test finding many documents.
      #
      # @example Test sending a find and exhausting the cursor.
      #   Benchmarking::MultiDoc.find_many(10)
      #
      # @param [ Integer ] repetitions The number of test repetitions.
      #
      # @return [ Numeric ] The median of the results.
      #
      # @since 2.2.3
      def find_many(repetitions)
        client.database.drop
        doc = Benchmarking.tweet_document

        10_000.times do |i|
          collection.insert_one(doc)
        end

        results = repetitions.times.collect do
          Benchmark.realtime do
            collection.find.to_a
          end
        end
        client.database.drop
        Benchmarking.median(results)
      end

      # Test doing a bulk insert of small documents.
      #
      # @example Test bulk insert of small documents.
      #   Benchmarking::MultiDoc.bulk_insert_small(10)
      #
      # @param [ Integer ] repetitions The number of test repetitions.
      #
      # @return [ Numeric ] The median of the results.
      #
      # @since 2.2.3
      def bulk_insert_small(repetitions)
        bulk_insert(repetitions, [Benchmarking.small_document] * 10_000)
      end

      # Test doing a bulk insert of large documents.
      #
      # @example Test bulk insert of large documents.
      #   Benchmarking::MultiDoc.bulk_insert_large(10)
      #
      # @param [ Integer ] repetitions The number of test repetitions.
      #
      # @return [ Numeric ] The median of the results.
      #
      # @since 2.2.3
      def bulk_insert_large(repetitions)
        bulk_insert(repetitions, [Benchmarking.large_document] * 10)
      end

      # Test uploading to GridFS.
      #
      # @example Test uploading to GridFS.
      #   Benchmarking::MultiDoc.gridfs_upload(10)
      #
      # @param [ Integer ] repetitions The number of test repetitions.
      #
      # @return [ Numeric ] The median of the results.
      #
      # @since 2.2.3
      def gridfs_upload(repetitions)
        client.database.drop
        create_collection
        fs = client.with(write_concern: { w: 1 }).database.fs(write_concern: { w: 1})

        s = StringIO.new('a')
        fs.upload_from_stream('create-indices.test', s)

        file = File.open(GRIDFS_FILE)

        results = repetitions.times.collect do
          file.rewind
          Benchmark.realtime do
            fs.upload_from_stream('GRIDFS_LARGE', file)
          end
        end
        Benchmarking.median(results)
      end

      # Test downloading from GridFS.
      #
      # @example Test downloading from GridFS.
      #   Benchmarking::MultiDoc.gridfs_download(10)
      #
      # @param [ Integer ] repetitions The number of test repetitions.
      #
      # @return [ Numeric ] The median of the results.
      #
      # @since 2.2.3
      def gridfs_download(repetitions = Benchmarking::TEST_REPETITIONS)
        client.database.drop
        create_collection
        fs = client.with(write_concern: { w: 1 }).database.fs(write_concern: { w: 1})

        file_id = fs.upload_from_stream('gridfstest', File.open(GRIDFS_FILE))
        io = StringIO.new

        results = repetitions.times.collect do
          io.rewind
          Benchmark.realtime do
            fs.download_to_stream(file_id, io)
          end
        end
        Benchmarking.median(results)
      end

      private

      def bulk_insert(repetitions, docs)
        client.database.drop
        create_collection

        results = repetitions.times.collect do
          Benchmark.realtime do
            collection.insert_many(docs)
          end
        end
        client.database.drop
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
