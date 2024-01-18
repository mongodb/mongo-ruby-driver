# frozen_string_literal: true

require_relative '../base'

module Mongo
  module DriverBench
    module SingleDoc
      class Base < Mongo::DriverBench::Base
        private

        attr_reader :client
        attr_reader :collection

        def setup
          if file_name
            @dataset ||= load_file(file_name).first
            @dataset_size ||= size_of_file(file_name)
          end

          prepare_client
        end

        def teardown
          cleanup_client
        end

        def prepare_client
          @client = new_client.use('perftest')
          @client.database.drop

          @collection = @client.database[:corpus].tap(&:create)
        end

        def cleanup_client
          @client.database.drop
        end

        # Returns the name of the file that contains
        # the dataset to use.
        def file_name
          nil
        end
      end
    end
  end
end
