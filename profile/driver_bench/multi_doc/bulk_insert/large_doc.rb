# frozen_string_literal: true

require_relative 'base'

module Mongo
  module DriverBench
    module MultiDoc
      module BulkInsert
        # "This benchmark tests driver performance inserting multiple, large
        # documents to the database."
        #
        # @api private
        class LargeDoc < Mongo::DriverBench::MultiDoc::BulkInsert::Base
          bench_name 'Large doc bulk insert'

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
