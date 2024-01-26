# frozen_string_literal: true

require_relative '../base'

module Mongo
  module DriverBench
    module BSON
      module Flat
        # Abstract superclass of flat BSON benchmarks.
        #
        # @api private
        class Base < Mongo::DriverBench::BSON::Base
          private

          # @return [ String ] the name of the file to use as the
          #   dataset for these benchmarks.
          def file_name
            'extended_bson/flat_bson.json'
          end
        end
      end
    end
  end
end
