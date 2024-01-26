# frozen_string_literal: true

require_relative 'base'
require_relative '../decodable'

module Mongo
  module DriverBench
    module BSON
      module Flat
        # "This benchmark tests driver performance decoding documents with top
        # level key/value pairs involving the most commonly-used BSON types."
        #
        # @api private
        class Decoding < Mongo::DriverBench::BSON::Flat::Base
          include Decodable

          bench_name 'Flat BSON Decoding'
        end
      end
    end
  end
end
