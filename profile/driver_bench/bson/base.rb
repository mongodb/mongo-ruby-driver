# frozen_string_literal: true

require_relative '../base'

module Mongo
  module DriverBench
    module BSON
      class Base < Mongo::DriverBench::Base
        private

        def setup
          @dataset ||= load_file(file_name).first
          @dataset_size ||= size_of_file(file_name)
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
