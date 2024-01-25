# frozen_string_literal: true

require_relative '../base'

module Mongo
  module DriverBench
    module BSON
      # Common behavior for "decode" benchmarks.
      #
      # @api private
      module Decodable
        private

        # The buffer to decode for the test
        attr_reader :buffer

        # Before executing the task itself.
        def before_task
          @buffer = ::BSON::Document.new(dataset).to_bson
        end

        # The decode operation, performed 10k times.
        def do_task
          10_000.times do
            ::BSON::Document.from_bson(buffer)
            buffer.rewind!
          end
        end
      end
    end
  end
end
