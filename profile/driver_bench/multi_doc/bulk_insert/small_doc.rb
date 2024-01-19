# frozen_string_literal: true

require_relative 'base'

module Mongo
  module DriverBench
    module MultiDoc
      module BulkInsert
        # "This benchmark tests driver performance inserting multiple, small
        # documents to the database."
        #
        # @api private
        class SmallDoc < Mongo::DriverBench::MultiDoc::BulkInsert::Base
          bench_name 'Small doc bulk insert'

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
