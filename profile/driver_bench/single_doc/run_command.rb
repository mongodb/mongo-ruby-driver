# frozen_string_literal: true

require_relative 'base'

module Mongo
  module DriverBench
    module SingleDoc
      class RunCommand < Mongo::DriverBench::SingleDoc::Base
        def setup
          super
          @dataset_size = { hello: true }.to_bson.length * scale
        end

        def prepare_client
          @client = new_client
        end

        def cleanup_client
          # do nothing
        end

        def do_task
          10_000.times do |i|
            client.database.command(hello: true)
          end
        end
      end
    end
  end
end
