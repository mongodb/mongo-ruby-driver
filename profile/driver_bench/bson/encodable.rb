# frozen_string_literal: true

require_relative '../base'

module Mongo
  module DriverBench
    module BSON
      module Encodable
        private

        # The document to encode for the test
        attr_reader :document

        def before_task
          @document = ::BSON::Document.new(dataset)
        end

        def do_task
          10_000.times { document.to_bson }
        end
      end
    end
  end
end
