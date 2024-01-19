# frozen_string_literal: true

require_relative '../base'

module Mongo
  module DriverBench
    module Parallel
      # Abstract base class for parallel micro-benchmarks.
      #
      # @api private
      class Base < Mongo::DriverBench::Base
        private

        attr_reader :client

        def setup
          prepare_client
        end

        def teardown
          cleanup_client
        end

        def prepare_client
          @client = new_client.use('perftest')
          @client.database.drop
        end

        def cleanup_client
          client.database.drop
        end
      end
    end
  end
end
