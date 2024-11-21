# frozen_string_literal: true

require_relative 'base'

module Mongo
  module DriverBench
    module SingleDoc
      module InsertOne
        # "This benchmark tests driver performance inserting a single, small
        # document to the database."
        #
        # @api private
        class SmallDoc < Mongo::DriverBench::SingleDoc::InsertOne::Base
          bench_name 'Small doc insertOne'

          def initialize
            super
            @repetitions = 10_000
          end

          def file_name
            'single_and_multi_document/small_doc.json'
          end
        end
      end
    end
  end
end
