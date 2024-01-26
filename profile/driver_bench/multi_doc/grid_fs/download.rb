# frozen_string_literal: true

require 'stringio'
require_relative 'base'

module Mongo
  module DriverBench
    module MultiDoc
      module GridFS
        # "This benchmark tests driver performance downloading a GridFS file
        # to memory."
        #
        # @api private
        class Download < Mongo::DriverBench::MultiDoc::GridFS::Base
          bench_name 'GridFS Download'

          private

          attr_reader :fs_bucket, :file_id

          def setup
            super

            @file_id = client.database.fs
                             .upload_from_stream 'gridfstest', dataset
          end

          def before_task
            super

            @fs_bucket = client.database.fs
          end

          def do_task
            fs_bucket.download_to_stream(file_id, StringIO.new)
          end
        end
      end
    end
  end
end
