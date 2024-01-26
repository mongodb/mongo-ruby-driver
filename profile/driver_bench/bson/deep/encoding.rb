# frozen_string_literal: true

require_relative 'base'
require_relative '../encodable'

module Mongo
  module DriverBench
    module BSON
      module Deep
        # "This benchmark tests driver performance encoding documents with
        # deeply nested key/value pairs involving subdocuments, strings,
        # integers, doubles and booleans."
        #
        # @api private
        class Encoding < Mongo::DriverBench::BSON::Deep::Base
          include Encodable

          bench_name 'Deep BSON Encoding'
        end
      end
    end
  end
end
