# frozen_string_literal: true

require_relative 'parallel/gridfs'
require_relative 'parallel/ldjson'

module Mongo
  module DriverBench
    module Parallel
      ALL = [ *GridFS::ALL, *LDJSON::ALL ].freeze

      # ParallelBench consists of all Parallel micro-benchmarks
      BENCH = ALL
    end
  end
end
