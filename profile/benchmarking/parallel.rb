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
    # @since 2.2.3
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
      # @since 2.2.3
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
      # @since 2.2.3
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
      # @since 2.2.3
      def import
        require 'yajl/json_gem'
        require 'celluloid'

        Mongo::Collection.send(:include, Celluloid)
        Celluloid.boot

        client.database.drop
        create_collection
        files =  [*0..99].collect { |i| "#{LDJSON_FILE_BASE}#{i.to_s.rjust(3, "0")}.txt" }

        result = Benchmark.realtime do
          Benchmarking::TEST_REPETITIONS.times do |i|
            docs = File.open(files[i]).map{ |document| JSON.parse(document) }
            collection.async.insert_many(docs)
          end
        end
        client.database.drop
        result
      end

      # Test concurrently exporting documents from a collection to a set of files.
      #
      # @example Testing concurrently importing files.
      #   Benchmarking::Parallel.export
      #
      # @return [ Numeric ] The test result.
      #
      # @since 2.2.3
      def export
        #require 'ruby-prof'
        insert_files
        files =  [*1..Benchmarking::TEST_REPETITIONS].collect do |i|
          name = "#{LDJSON_FILE_OUTPUT_BASE}#{i.to_s.rjust(3, "0")}.txt"
          File.new(name, 'w')
        end
        #prof = nil
        result = Benchmark.realtime do
          Benchmarking::TEST_REPETITIONS.times do |i|
            #prof = RubyProf.profile do
            files[i].write(collection.find(_id: { '$gte' => (i * 5000),
                                                  '$lt' => (i+1) * 5000 }).to_a)
            end
          #end
        end
        client.database.drop
        result
      end

      # This benchmark tests driver performance uploading files from disk to GridFS.
      #
      # @example Test uploading files from disk to GridFS.
      #   Benchmarking::Parallel.gridfs_upload
      #
      # @return [ Numeric ] The test result.
      #
      # @since 2.2.3
      def gridfs_upload
        n = 50
        client.database.drop
        fs = client.database.fs

        files =  [*0...n].collect do |i|
          name = "#{GRIDFS_MULTI_BASE}#{i}.txt"
          {
            file: File.open(name, 'r'),
            name: File.basename(name)
          }
        end

        s = StringIO.new('a')
        fs.upload_from_stream('create-indices.test', s)

        Benchmark.realtime do
          n.times do |i|
            fs.upload_from_stream(files[i][:name], files[i][:file])
          end
        end
      end
      alias :gridfs_upload_jruby :gridfs_upload


      # This benchmark tests driver performance downloading files from GridFS to disk.
      #
      # @example Test downloading files from GridFS to disk.
      #   Benchmarking::Parallel.gridfs_download
      #
      # @return [ Numeric ] The test result.
      #
      # @since 2.2.3
      def gridfs_download
        n_files = 50
        n_threads = BSON::Environment.jruby? ? 4 : 2
        threads = []
        client.database.drop
        fs = client.database.fs

        file_info =  [*0...n_files].collect do |i|
          name = "#{GRIDFS_MULTI_BASE}#{i}.txt"
          {
            _id: fs.upload_from_stream(name, File.open(name)),
            output_name: "#{GRIDFS_MULTI_OUTPUT_BASE}#{i}.txt"
          }
        end.freeze

        reps = n_files/n_threads.freeze
        Benchmark.realtime do
          n_threads.times do |i|
            threads << Thread.new do
              reps.times do |j|
                index = i * reps + j
                fs.download_to_stream(file_info[index][:_id], File.open(file_info[index][:output_name], "w"))
              end
            end
          end
          threads.collect(&:value)
        end
      end
      alias :gridfs_download_jruby :gridfs_download

      private

      def insert_files
        require 'yajl/json_gem'
        require 'celluloid'

        Mongo::Collection.send(:include, Celluloid)

        client.database.drop
        create_collection
        files =  [*1..Benchmarking::TEST_REPETITIONS].collect do |i|
          File.open("#{LDJSON_FILE_BASE}#{i.to_s.rjust(3, "0")}.txt")
        end

        Benchmarking::TEST_REPETITIONS.times do |i|
          docs = files[i].each_with_index.collect do |document, offset|
            JSON.parse(document).merge(_id: i * 5000 + offset)
          end
          collection.async.insert_many(docs)
        end
        puts "Imported #{Benchmarking::TEST_REPETITIONS} files, #{collection.count} documents."
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
