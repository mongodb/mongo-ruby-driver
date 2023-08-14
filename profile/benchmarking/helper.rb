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
        doc['_id'] = BSON::ObjectId.from_string(doc['_id']['$oid']) if doc['_id'] && doc['_id']['$oid']
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
    def benchmark(max_iterations: Benchmarking::TEST_REPETITIONS, min_time: 60, max_time: 5 * 60, &block)
      [].tap do |results|
        iteration_count = 0
        cumulative_time = 0

        loop do
          timing = Benchmark.realtime(&block)

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
      end
    end

    # Formats and displays a report of the given results.
    #
    # @param [ Hash ] results the results of a benchmarking run.
    # @param [ Integer ] indent how much the report should be indented.
    # @param [ Array<Number> ] percentiles the percentile values to report
    def report(results, indent: 0, percentiles: [ 10, 25, 50, 75, 90, 95, 98, 99 ])
      indentation = ' ' * indent
      results.each do |key, value|
        puts "#{indentation}#{key}:"
        if value.is_a?(Hash)
          report(value, indent: indent + 2, percentiles: percentiles)
        else
          ps = Percentiles.new(value)
          puts "#{indentation}  median: %g" % [ ps[50] ]
          puts "#{indentation}  percentiles:"
          percentiles.each do |pct|
            puts "#{indentation}    %g: %g" % [ pct, ps[pct] ]
          end
        end
      end
    end

    # A utility class for returning the list item at a given percentile
    # value.
    class Percentiles
      # @return [ Array<Number> ] the sorted list of numbers to consider
      attr_reader :list

      # Create a new Percentiles object that encapsulates the given list of
      # numbers.
      #
      # @param [ Array<Number> ] list the list of numbers to considier
      def initialize(list)
        @list = list.sort
      end

      # Finds and returns the element in the list that represents the given
      # percentile value.
      #
      # @param [ Number ] percentile a number in the range [1,100]
      #
      # @return [ Number ] the element of the list for the given percentile.
      def [](percentile)
        i = (list.size * percentile / 100.0).ceil - 1
        list[i]
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
  end
end
