# frozen_string_literal: true

require_relative '../base'

module Mongo
  module DriverBench
    module BSON
      # Common behavior for the "encode" benchmarks.
      #
      # @api private
      module Encodable
        private

        # The document to encode for the test
        attr_reader :document

        # Before each task.
        def before_task
          @document = ::BSON::Document.new(dataset)
        end

        # The encode operation itself, executed 10k times.
        def do_task
          10_000.times { document.to_bson }
        end
      end
    end
  end
end
