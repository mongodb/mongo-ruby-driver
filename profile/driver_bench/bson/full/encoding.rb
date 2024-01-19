# frozen_string_literal: true

require_relative 'base'
require_relative '../encodable'

module Mongo
  module DriverBench
    module BSON
      module Full
        # "This benchmark tests driver performance encoding documents with top
        # level key/value pairs involving the full range of BSON types."
        #
        # @api private
        class Encoding < Mongo::DriverBench::BSON::Full::Base
          include Encodable

          bench_name 'Full BSON Encoding'
        end
      end
    end
  end
end
