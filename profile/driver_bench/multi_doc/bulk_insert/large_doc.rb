# frozen_string_literal: true

require_relative 'base'

module Mongo
  module DriverBench
    module MultiDoc
      module BulkInsert
        class LargeDoc < Mongo::DriverBench::MultiDoc::BulkInsert::Base
          def initialize
            super
            @repetitions = 10
          end

          def file_name
            "single_and_multi_document/large_doc.json"
          end
        end
      end
    end
  end
end
