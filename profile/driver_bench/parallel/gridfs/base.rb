# frozen_string_literal: true

require_relative '../base'

module Mongo
  module DriverBench
    module Parallel
      module GridFS
        class Base < Mongo::DriverBench::Parallel::Base
          def file_name_at(index)
            format("parallel/gridfs_multi/file%02d.txt", index)
          end

          private

          attr_reader :bucket

          def prepare_bucket(initialize: true)
            @bucket = client.database.fs
            @bucket.drop
            @bucket.upload_from_stream "one-byte-file", "\n" if initialize
          end

          def upload_file(file_name)
            File.open(path_to_file(file_name), 'r') do |file|
              bucket.upload_from_stream file_name, file
            end
          end
        end
      end
    end
  end
end
