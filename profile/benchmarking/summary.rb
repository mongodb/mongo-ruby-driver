# frozen_string_literal: true

module Mongo
  module Benchmarking
    # A utility class for encapsulating the summary information for a
    # benchmark, including behaviors for reporting on the summary.
    class Summary
      # @return [ Array<Numeric> ] the timings of each iteration in the
      #   benchmark
      attr_reader :timings

      # @return [ Percentiles ] the percentiles object for querying the
      #   timing at a given percentile value.
      attr_reader :percentiles

      # @return [ Numeric ] the composite score for the benchmark
      attr_reader :score

      # Construct a new Summary object with the given timings, percentiles,
      # and score.
      #
      # @param [ Array<Numeric> ] timings the timings of each iteration in the
      #   benchmark
      # @param [ Percentiles ] percentiles the percentiles object for querying
      #   the timing at a given percentile value
      # @param [ Numeric ] score the composite score for the benchmark
      def initialize(timings, percentiles, score)
        @timings = timings
        @percentiles = percentiles
        @score = score
      end

      # @return [ Numeric ] the median timing for the benchmark.
      def median
        percentiles[50]
      end

      # Formats and displays the results of a single benchmark run.
      #
      # @param [ Integer ] indent how much the report should be indented
      # @param [ Array<Numeric> ] points the percentile points to report
      #
      # @return [ String ] a YAML-formatted summary
      def summary(indent, points)
        [].tap do |lines|
          lines << format('%*sscore: %g', indent, '', score)
          lines << format('%*smedian: %g', indent, '', median)
          lines << format('%*spercentiles:', indent, '')
          points.each do |pct|
            lines << format('%*s%g: %g', indent + 2, '', pct, percentiles[pct])
          end
        end.join("\n")
      end
    end
  end
end
