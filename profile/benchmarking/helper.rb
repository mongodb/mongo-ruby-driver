# frozen_string_literal: true

module Mongo
  # Helper functions used by benchmarking tasks
  module Benchmarking
    extend self

    # Load a json file and represent each document as a Hash.
    #
    # @example Load a file.
    #   Benchmarking.load_file(file_name)
    #
    # @param [ String ] The file name.
    #
    # @return [ Array ] A list of extended-json documents.
    #
    # @since 2.2.3
    def load_file(file_name)
      File.open(file_name, 'r') do |f|
        f.each_line.collect do |line|
          parse_json(line)
        end
      end
    end

    # Load a json document as a Hash and convert BSON-specific types.
    # Replace the _id field as an BSON::ObjectId if it's represented as '$oid'.
    #
    # @example Parse a json document.
    #   Benchmarking.parse_json(document)
    #
    # @param [ Hash ] The json document.
    #
    # @return [ Hash ] An extended-json document.
    #
    # @since 2.2.3
    def parse_json(document)
      JSON.parse(document).tap do |doc|
        doc['_id'] = ::BSON::ObjectId.from_string(doc['_id']['$oid']) if doc['_id'] && doc['_id']['$oid']
      end
    end

    # The spec requires that most benchmarks use a variable number of
    # iterations, defined as follows:
    #
    # * iterations should loop for at least 1 minute cumulative execution
    #   time
    # * iterations should stop after 100 iterations or 5 minutes cumulative
    #   execution time, whichever is shorter
    #
    # This method will yield once for each iteration.
    #
    # @param [ Integer ] max_iterations the maximum number of iterations to
    #   attempt (default: 100)
    # @param [ Integer ] min_time the minimum number of seconds to spend
    #   iterating
    # @param [ Integer ] max_time the maximum number of seconds to spend
    #   iterating.
    #
    # @return [ Array<Float> ] the timings for each iteration
    def benchmark(max_iterations: Benchmarking::TEST_REPETITIONS,
                  min_time: 60,
                  max_time: 5 * 60,
                  progress: default_progress_callback,
                  &block)
      progress ||= ->(state) {} # fallback to a no-op callback
      progress[:start]

      [].tap do |results|
        iteration_count = 0
        cumulative_time = 0

        loop do
          timing = without_gc { Benchmark.realtime(&block) }
          progress[:step]

          iteration_count += 1
          cumulative_time += timing
          results.push timing

          # always stop after the maximum time has elapsed, regardless of
          # iteration count.
          break if cumulative_time > max_time

          # otherwise, break if the minimum time has elapsed, and the maximum
          # number of iterations have been reached.
          break if cumulative_time >= min_time && iteration_count >= max_iterations
        end

        progress[:end]
      end
    end

    # Formats and displays a report of the given results.
    #
    # @param [ Hash ] results the results of a benchmarking run.
    # @param [ Integer ] indent how much the report should be indented.
    # @param [ Array<Numeric> ] percentiles the percentile values to report
    def report(results, indent: 0, percentiles: [ 10, 25, 50, 75, 90, 95, 98, 99 ])
      results.each do |key, value|
        puts format('%*s%s:', indent, '', key)

        if value.respond_to?(:summary)
          puts value.summary(indent + 2, percentiles)
        else
          report(value, indent: indent + 2, percentiles: percentiles)
        end
      end
    end

    # Get the median of values in a list.
    #
    # @example Get the median.
    #   Benchmarking.median(values)
    #
    # @param [ Array ] values The values to get the median of.
    #
    # @return [ Numeric ] The median of the list.
    def median(values)
      i = (values.size / 2) - 1
      values.sort[i]
    end

    # Runs a given block with GC disabled.
    def without_gc
      GC.disable
      yield
    ensure
      GC.enable
    end

    private

    # Returns the proc object (or nil) corresponding to the "PROGRESS"
    # environment variable.
    #
    # @return [ Proc | nil ] the callback proc to use (or nil if none should
    #   be used)
    def default_progress_callback
      case ENV['PROGRESS']
      when '0', 'false', 'none'
        nil
      when nil, '1', 'true', 'minimal'
        method(:minimal_progress_callback).to_proc
      else
        raise ArgumentError, "unsupported progress callback #{ENV['PROGRESS'].inspect}"
      end
    end

    # A minimal progress callback implementation, printing '|' when a benchmark
    # starts and '.' for each iteration.
    #
    # @param [ :start | :step | :end ] state the current progress state
    def minimal_progress_callback(state)
      case state
      when :start then print '|'
      when :step  then print '.'
      when :end   then puts
      end

      $stdout.flush
    end
  end
end
