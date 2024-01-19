# frozen_string_literal: true

require_relative 'base'
require_relative 'upload'
require_relative '../dispatcher'

module Mongo
  module DriverBench
    module Parallel
      module GridFS
        # This benchmark tests driver performance downloading files from
        # GridFS to disk.
        #
        # @api private
        class Download < Mongo::DriverBench::Parallel::GridFS::Base
          bench_name 'GridFS multi-file download'

          private

          # The source object to use for this benchmark. Each batch is a tuple
          # consisting of the list position, and the element in the list at
          # that position.
          #
          # @api private
          class Source
            def initialize(list)
              @list = list
              @n = 0
            end

            def next
              id = @list.pop or return nil
              [ @n, id ].tap { @n += 1 }
            end
          end

          def setup
            super
            prepare_bucket(initialize: false)

            dispatcher = Dispatcher.new(Upload::Source.new(self)) do |file_name|
              upload_file(file_name)
            end
            dispatcher.run

            @destination = File.join(Dir.tmpdir, 'parallel')
          end

          def before_task
            super
            FileUtils.rm_rf(@destination)
            FileUtils.mkdir_p(@destination)

            ids = bucket.files_collection.find.map { |doc| doc['_id'] }
            @dispatcher = Dispatcher.new(Source.new(ids)) do |(n, id)|
              download_file(n, id)
            end
          end

          def do_task
            @dispatcher.run
          end

          def download_file(index, id)
            path = File.join(@destination, file_name_at(index))
            FileUtils.mkdir_p(File.dirname(path))

            File.open(path, 'w') do |file|
              bucket.download_to_stream(id, file)
            end
          end
        end
      end
    end
  end
end
