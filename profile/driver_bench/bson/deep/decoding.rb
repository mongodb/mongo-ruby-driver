# frozen_string_literal: true

require_relative 'base'
require_relative '../decodable'

module Mongo
  module DriverBench
    module BSON
      module Deep
        # "This benchmark tests driver performance decoding documents with
        # deeply nested key/value pairs involving subdocuments, strings,
        # integers, doubles and booleans."
        #
        # @api private
        class Decoding < Mongo::DriverBench::BSON::Deep::Base
          include Decodable

          bench_name 'Deep BSON Decoding'
        end
      end
    end
  end
end
