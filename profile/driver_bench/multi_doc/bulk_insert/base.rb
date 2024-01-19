# frozen_string_literal: true

require_relative '../base'

module Mongo
  module DriverBench
    module MultiDoc
      module BulkInsert
        # Abstract superclass for all bulk insert benchmarks.
        #
        # @api private
        class Base < Mongo::DriverBench::MultiDoc::Base
          attr_reader :repetitions, :bulk_dataset

          def setup
            super
            @bulk_dataset = dataset * repetitions
          end

          # How much the benchmark's dataset size ought to be scaled (for
          # scoring purposes).
          def scale
            @repetitions
          end

          def before_task
            collection.drop
            collection.create
          end

          def do_task
            collection.insert_many(bulk_dataset, ordered: true)
          end
        end
      end
    end
  end
end
