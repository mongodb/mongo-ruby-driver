# frozen_string_literal: true

require 'benchmark'
require 'mongo'

require_relative 'configuration'
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
        @max_iterations = Integer(ENV['DRIVER_BENCH_MAX_ITERATIONS'] || (debug_mode? ? 10 : 100))
        @min_time = Float(ENV['DRIVER_BENCH_MIN_TIME'] || (debug_mode? ? 1 : 60))
        @max_time = 300 # 5 minutes
      end

      # The number of driver operations one iteration performs, or nil when
      # the task has no meaningful per-operation unit. Per-operation metrics
      # are only reported for tasks that define it.
      #
      # @return [ Integer | nil ] operations per iteration.
      def ops_per_iteration
        nil
      end

      def debug_mode?
        ENV['PERF_DEBUG']
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
          configuration: Configuration.current.name,
          score: score,
          percentiles: percentiles,
          per_op: per_op_metrics(percentiles) }
      end

      # Runs one iteration with every started span counted, and returns the
      # number of spans per operation. Kept apart from #run: counting needs a
      # span processor, which the timed runs deliberately do without.
      #
      # @param counter [ #reset, #count ] the span counter the tracer
      #   provider reports to.
      #
      # @return [ Float | nil ] spans per operation, or nil when the task
      #   defines no per-operation unit.
      def count_spans(counter)
        return nil unless ops_per_iteration

        setup
        before_task
        counter.reset
        do_task
        spans = counter.count
        after_task
        teardown
        spans.to_f / ops_per_iteration
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
      #
      def run_benchmark
        [].tap do |timings|
          iteration_count = 0
          cumulative_time = 0

          setup

          @cpu_times = []
          @gc_times = []
          @allocations = []

          loop do
            before_task
            timing = consider_gc { measure_iteration { debug_mode? ? sleep(0.1) : do_task } }
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

      # Times one iteration, and records alongside the wall-clock time the
      # process CPU time and the number of objects allocated. CPU time leaves
      # out the time spent waiting on the server, and allocations are nearly
      # deterministic, so both show driver-side cost with far less noise than
      # throughput. Process CPU time includes the driver's background threads
      # (e.g. server monitoring), which cost the same in every configuration.
      #
      # @return [ Float ] the wall-clock time in seconds.
      #
      # GC time is recorded too, where the runtime reports it (Ruby 3.1+).
      # When and how long the collector runs varies between processes more
      # than the cost of tracing does, so CPU time without GC is the steadier
      # measure of the driver's own work.
      def measure_iteration(&block)
        allocated = GC.stat(:total_allocated_objects)
        gc = gc_time
        cpu = Process.clock_gettime(Process::CLOCK_PROCESS_CPUTIME_ID)
        timing = Benchmark.realtime(&block)
        @cpu_times.push(Process.clock_gettime(Process::CLOCK_PROCESS_CPUTIME_ID) - cpu)
        @gc_times.push(gc_time - gc) if gc
        @allocations.push(GC.stat(:total_allocated_objects) - allocated)
        timing
      end

      # @return [ Float | nil ] the total time spent in GC so far, in
      #   seconds, or nil when the runtime does not report it.
      def gc_time
        ms = GC.stat[:time]
        ms && (ms / 1000.0)
      end

      # Per-operation medians, for tasks that define ops_per_iteration.
      #
      # @return [ Hash<String, Float> ] metric name to value.
      def per_op_metrics(timings)
        return {} unless ops_per_iteration

        ops = ops_per_iteration.to_f
        {
          'wall_us_per_op' => timings[50] / ops * 1_000_000,
          'cpu_us_per_op' => Percentiles.new(@cpu_times)[50] / ops * 1_000_000,
          'allocs_per_op' => Percentiles.new(@allocations)[50] / ops
        }.merge(gc_metrics(ops))
      end

      def gc_metrics(ops)
        return {} if @gc_times.empty?

        cpu_ex_gc = @cpu_times.zip(@gc_times).map { |cpu, gc| cpu - gc }
        {
          'gc_us_per_op' => Percentiles.new(@gc_times)[50] / ops * 1_000_000,
          'cpu_ex_gc_us_per_op' => Percentiles.new(cpu_ex_gc)[50] / ops * 1_000_000
        }
      end

      # Instantiate a new client.
      def new_client(uri = ENV['MONGODB_URI'])
        Mongo::Client.new(uri, Configuration.current.client_options)
      end

      # Takes care of garbage collection considerations before
      # running the block.
      #
      # Set BENCHMARK_NO_GC environment variable to suppress GC during
      # the core benchmark tasks; note that this may result in obscure issues
      # due to memory pressures on larger benchmarks.
      def consider_gc
        GC.start
        GC.disable if ENV['BENCHMARK_NO_GC']
        yield
      ensure
        GC.enable if ENV['BENCHMARK_NO_GC']
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
      def setup; end

      # Executed before each iteration of the benchmark.
      def before_task; end

      # Smallest amount of code necessary to do the task,
      # invoked once per iteration.
      def do_task
        raise NotImplementedError
      end

      # Executed after each iteration of the benchmark.
      def after_task; end

      # Executed at the end of the micro-benchmark.
      def teardown; end
    end
  end
end
