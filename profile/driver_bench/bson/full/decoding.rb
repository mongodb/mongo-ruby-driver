# frozen_string_literal: true

require_relative 'base'
require_relative '../decodable'

module Mongo
  module DriverBench
    module BSON
      module Full
        # "This benchmark tests driver performance decoding documents with top
        # level key/value pairs involving the full range of BSON types."
        #
        # @api private
        class Decoding < Mongo::DriverBench::BSON::Full::Base
          include Decodable

          bench_name 'Full BSON Decoding'
        end
      end
    end
  end
end
