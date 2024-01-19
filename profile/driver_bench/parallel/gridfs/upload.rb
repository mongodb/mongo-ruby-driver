# frozen_string_literal: true

require_relative 'base'
require_relative '../dispatcher'

module Mongo
  module DriverBench
    module Parallel
      module GridFS
        # "This benchmark tests driver performance uploading files from disk
        # to GridFS."
        #
        # @api private
        class Upload < Mongo::DriverBench::Parallel::GridFS::Base
          bench_name 'GridFS multi-file upload'

          # The source object to use for this benchmark. Each batch consists
          # of the name of the file to upload.
          #
          # @api private
          class Source
            def initialize(bench)
              @n = 0
              @bench = bench
            end

            def next
              return nil if @n >= 50

              @bench.file_name_at(@n).tap { @n += 1 }
            end
          end

          private

          def before_task
            super
            prepare_bucket
            @dispatcher = Dispatcher.new(Source.new(self)) do |file_name|
              upload_file(file_name)
            end
          end

          def do_task
            @dispatcher.run
          end
        end
      end
    end
  end
end
