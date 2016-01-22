# Copyright (C) 2015 MongoDB, Inc.
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

#require 'ruby-prof'

module Mongo
  module Benchmarking

    # Parallel tests simulate ETL operations from disk to database or vice-versa.
    # They are designed to be implemented using a language's preferred approach to
    # concurrency and thus stress how drivers handle concurrency.
    # These intentionally involve overhead above and beyond the driver itself to
    # simulate the sort of "real-world" pressures that a drivers would be under
    # during concurrent operation.
    #
    # @since 2.2.2
    module Parallel

      extend self

      # Run a parallel benchmark test.
      #
      # @example Run a test.
      #   Benchmarking::Parallel.run(:import)
      #
      # @param [ Symbol ] type The type of test to run.
      #
      # @return [ Numeric ] The test results.
      #
      # @since 2.2.2
      def run(type)
        Mongo::Logger.logger.level = ::Logger::WARN
        type = type.to_s + '_jruby' if BSON::Environment.jruby?
        puts "#{type} : #{send(type)}"
      end

      # Test concurrently importing documents from a set of files.
      # Using JRuby.
      #
      # @example Testing concurrently importing files using JRuby.
      #   Benchmarking::Parallel.import_jruby
      #
      # @return [ Numeric ] The test result.
      #
      # @since 2.2.2
      def import_jruby
        #require 'jrjackson'
        client.database.drop
        create_collection
        files =  [*1..100].collect { |i| "#{LDJSON_FILE_BASE}#{i.to_s.rjust(3, "0")}.txt" }

        threads = []
        result = Benchmark.realtime do
          4.times do |i|
            threads << Thread.new do
              25.times do |j|
                docs = File.open(files[10 * i + j]).collect { |document| JSON.parse(document) }
                #docs = File.open(files[10 * i + j]).collect { |document| JrJackson::Json.load(document) }
                collection.insert_many(docs)
              end
            end
          end
          threads.collect { |t| t.join }
        end
        client.database.drop
        result
      end

      # Test concurrently importing documents from a set of files.
      #
      # @example Testing concurrently importing files.
      #   Benchmarking::Parallel.import
      #
      # @return [ Numeric ] The test result.
      #
      # @since 2.2.2
      def import
        #require 'yajl'
        #parser = Yajl::Parser.new
        client.database.drop
        create_collection
        files =  [*1..100].collect { |i| "#{LDJSON_FILE_BASE}#{i.to_s.rjust(3, "0")}.txt" }

        threads = []
        result = Benchmark.realtime do
          4.times do |i|
            threads << Thread.new do
              10.times do |j|
                docs = File.open(files[10 * i + j]).collect { |document| JSON.parse(document) }
                #docs = File.open(files[10 * i + j]).collect { |document| parser.parse(document) }
                collection.insert_many(docs)
              end
            end
          end
          threads.collect { |t| t.join }
        end
        client.database.drop
        result
      end

      private

      def client
        @client ||= Mongo::Client.new(["localhost:27017"], database: 'perftest', monitoring: false)
      end

      def collection
        @collection ||= client[:corpus].tap { |coll| coll.create }
      end
      alias :create_collection :collection
    end
  end
end
