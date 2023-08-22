# frozen_string_literal: true

module Mongo
  module Benchmarking
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
  end
end
