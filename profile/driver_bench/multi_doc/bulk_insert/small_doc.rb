# frozen_string_literal: true

require_relative 'base'

module Mongo
  module DriverBench
    module MultiDoc
      module BulkInsert
        class SmallDoc < Mongo::DriverBench::MultiDoc::BulkInsert::Base
          def initialize
            super
            @repetitions = 10_000
          end

          def file_name
            "single_and_multi_document/small_doc.json"
          end
        end
      end
    end
  end
end
