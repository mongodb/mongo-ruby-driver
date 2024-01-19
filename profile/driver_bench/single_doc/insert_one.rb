# frozen_string_literal: true

require_relative 'insert_one/large_doc'
require_relative 'insert_one/small_doc'

module Mongo
  module DriverBench
    module SingleDoc
      module InsertOne
        ALL = [ LargeDoc, SmallDoc ].freeze
      end
    end
  end
end
