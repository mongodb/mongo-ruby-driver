# frozen_string_literal: true

require_relative 'base'

module Mongo
  module DriverBench
    module SingleDoc
      module InsertOne
        # "This benchmark tests driver performance inserting a single, large
        # document to the database."
        #
        # @api private
        class LargeDoc < Mongo::DriverBench::SingleDoc::InsertOne::Base
          bench_name 'Large doc insertOne'

          def initialize
            super
            @repetitions = 10
          end

          def file_name
            'single_and_multi_document/large_doc.json'
          end
        end
      end
    end
  end
end
