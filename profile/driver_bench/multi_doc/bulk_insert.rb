# frozen_string_literal: true

require_relative 'bulk_insert/large_doc'
require_relative 'bulk_insert/small_doc'

module Mongo
  module DriverBench
    module MultiDoc
      module BulkInsert
        ALL = [ LargeDoc, SmallDoc ].freeze
      end
    end
  end
end
