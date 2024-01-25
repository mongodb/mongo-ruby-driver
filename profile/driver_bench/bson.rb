# frozen_string_literal: true

require_relative 'bson/deep'
require_relative 'bson/flat'
require_relative 'bson/full'

module Mongo
  module DriverBench
    module BSON
      ALL = [ *Deep::ALL, *Flat::ALL, *Full::ALL ].freeze

      # BSONBench consists of all BSON micro-benchmarks
      BENCH = ALL
    end
  end
end
