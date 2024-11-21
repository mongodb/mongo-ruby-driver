# frozen_string_literal: true

require_relative 'base'
require_relative '../encodable'

module Mongo
  module DriverBench
    module BSON
      module Flat
        # "This benchmark tests driver performance encoding documents with top
        # level key/value pairs involving the most commonly-used BSON types."
        #
        # @api private
        class Encoding < Mongo::DriverBench::BSON::Flat::Base
          include Encodable

          bench_name 'Flat BSON Encoding'
        end
      end
    end
  end
end
