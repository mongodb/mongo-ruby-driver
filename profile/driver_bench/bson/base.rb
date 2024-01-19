# frozen_string_literal: true

require_relative '../base'

module Mongo
  module DriverBench
    module BSON
      # Abstract superclass for all BSON benchmarks.
      #
      # @api private
      class Base < Mongo::DriverBench::Base
        private

        # Common setup for these benchmarks.
        def setup
          @dataset ||= load_file(file_name).first
          @dataset_size ||= size_of_file(file_name) * 10_000
        end

        # Returns the name of the file name that contains
        # the dataset to use.
        def file_name
          raise NotImplementedError
        end
      end
    end
  end
end
