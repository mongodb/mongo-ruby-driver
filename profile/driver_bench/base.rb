# frozen_string_literal: true

require 'benchmark'
require 'mongo'

require_relative 'percentiles'

module Mongo
  module DriverBench
    # Base class for DriverBench profile benchmarking classes.
    #
    # @api private
    class Base
      # A convenience for setting and querying the benchmark's name
      def self.bench_name(benchmark_name = nil)
        @bench_name = benchmark_name if benchmark_name
        @bench_name
      end

      # Where to look for the data files
      DATA_PATH = File.expand_path('../data/driver_bench', __dir__)

      # The maximum number of iterations to perform when executing the
      # micro-benchmark.
      attr_reader :max_iterations

      # The minimum number of seconds that the micro-benchmark must run,
      # regardless of how many iterations it takes.
      attr_reader :min_time

      # The maximum number of seconds that the micro-benchmark must run,
      # regardless of how many iterations it takes.
      attr_reader :max_time

      # The dataset to be used by the micro-benchmark.
      attr_reader :dataset

      # The size of the dataset, computed per the spec, to be
      # used for scoring the results.
      attr_reader :dataset_size

      # Instantiate a new micro-benchmark class.
      def initialize
        @max_iterations = 100
        @min_time = ENV['CHEAT'] ? 10 : 60
        @max_time = 300 # 5 minutes
      end

      # Runs the benchmark and returns the score.
      #
      # @return [ Hash<name,score,percentiles> ] the score and other
      #   attributes of the benchmark.
      def run
        timings = run_benchmark
        percentiles = Percentiles.new(timings)
        score = dataset_size / percentiles[50] / 1_000_000.0

        { name: self.class.bench_name,
          score: score,
          percentiles: percentiles }
      end

      private

      # Runs the micro-benchmark, and returns an array of timings, with one
      # entry for each iteration of the benchmark. It may have fewer than
      # max_iterations entries if it takes longer than max_time seconds, or
      # more than max_iterations entries if it would take less than min_time
      # seconds to run.
      #
      # @return [ Array<Float> ] the array of timings (in seconds) for
      #   each iteration.
      def run_benchmark
        [].tap do |timings|
          iteration_count = 0
          cumulative_time = 0

          setup

          loop do
            before_task
            timing = without_gc { Benchmark.realtime { ENV['CHEAT'] ? sleep(0.1) : do_task } }
            after_task

            iteration_count += 1
            cumulative_time += timing
            timings.push timing

            # always stop after the maximum time has elapsed, regardless of
            # iteration count.
            break if cumulative_time > max_time

            # otherwise, break if the minimum time has elapsed, and the maximum
            # number of iterations have been reached.
            break if cumulative_time >= min_time && iteration_count >= max_iterations
          end

          teardown
        end
      end

      # Instantiate a new client.
      def new_client(uri = ENV['MONGODB_URI'])
        Mongo::Client.new(uri)
      end

      # Runs a given block with GC disabled.
      def without_gc
        GC.disable
        yield
      ensure
        GC.enable
      end

      # By default, the file name is assumed to be relative to the
      # DATA_PATH, unless the file name is an absolute path.
      def path_to_file(file_name)
        return file_name if file_name.start_with?('/')
        File.join(DATA_PATH, file_name)
      end

      # Load a json file and represent each document as a Hash.
      #
      # @param [ String ] file_name The file name.
      #
      # @return [ Array ] A list of extended-json documents.
      def load_file(file_name)
        File.readlines(path_to_file(file_name)).map { |line| ::BSON::Document.new(parse_line(line)) }
      end

      # Returns the size (in bytes) of the given file.
      def size_of_file(file_name)
        File.size(path_to_file(file_name))
      end

      # Load a json document as a Hash and convert BSON-specific types.
      # Replace the _id field as an BSON::ObjectId if it's represented as '$oid'.
      #
      # @param [ String ] document The json document.
      #
      # @return [ Hash ] An extended-json document.
      def parse_line(document)
        JSON.parse(document).tap do |doc|
          doc['_id'] = ::BSON::ObjectId.from_string(doc['_id']['$oid']) if doc['_id'] && doc['_id']['$oid']
        end
      end

      # Executed at the start of the micro-benchmark.
      def setup
      end

      # Executed before each iteration of the benchmark.
      def before_task
      end

      # Smallest amount of code necessary to do the task,
      # invoked once per iteration.
      def do_task
        raise NotImplementedError
      end

      # Executed after each iteration of the benchmark.
      def after_task
      end

      # Executed at the end of the micro-benchmark.
      def teardown
      end
    end
  end
end
