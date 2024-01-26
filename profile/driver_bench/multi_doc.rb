# frozen_string_literal: true

require_relative 'multi_doc/bulk_insert'
require_relative 'multi_doc/find_many'
require_relative 'multi_doc/grid_fs'

module Mongo
  module DriverBench
    module MultiDoc
      ALL = [ *BulkInsert::ALL, FindMany, *GridFS::ALL ].freeze

      # MultiBench consists of all Multi-doc micro-benchmarks
      BENCH = ALL
    end
  end
end
