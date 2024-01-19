# frozen_string_literal: true

require_relative 'base'

module Mongo
  module DriverBench
    module SingleDoc
      module InsertOne
        class SmallDoc < Mongo::DriverBench::SingleDoc::InsertOne::Base
          def initialize
            super
            @repetitions = 10_000
          end

          def scale
            @repetitions
          end

          def file_name
            "single_and_multi_document/small_doc.json"
          end
        end
      end
    end
  end
end
