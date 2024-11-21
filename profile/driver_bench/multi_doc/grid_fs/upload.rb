# frozen_string_literal: true

require_relative 'base'

module Mongo
  module DriverBench
    module MultiDoc
      module GridFS
        # "This benchmark tests driver performance uploading a GridFS file
        # from memory."
        #
        # @api private
        class Upload < Mongo::DriverBench::MultiDoc::GridFS::Base
          bench_name 'GridFS Upload'

          private

          attr_reader :fs_bucket

          def before_task
            super

            @fs_bucket = client.database.fs
            @fs_bucket.drop

            @fs_bucket.upload_from_stream 'one-byte-file', "\n"
          end

          def do_task
            fs_bucket.upload_from_stream file_name, dataset
          end
        end
      end
    end
  end
end
