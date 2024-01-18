# frozen_string_literal: true

require_relative '../base'

module Mongo
  module DriverBench
    module MultiDoc
      module BulkInsert
        class Base < Mongo::DriverBench::MultiDoc::Base
          attr_reader :repetitions
          attr_reader :bulk_dataset

          def setup
            super
            @bulk_dataset = dataset * repetitions
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
