# frozen_string_literal: true

require 'tmpdir'
require 'fileutils'

require_relative 'base'
require_relative '../dispatcher'

module Mongo
  module DriverBench
    module Parallel
      module LDJSON
        # "This benchmark tests driver performance exporting documents to a
        # set of LDJSON files."
        #
        # @api private
        class Export < Mongo::DriverBench::Parallel::LDJSON::Base
          bench_name 'LDJSON multi-file export'

          private

          # The data source for this benchmark; each batch is a set of 5000
          # documents.
          class DataSource
            def initialize(collection)
              @n = 0
              @collection = collection
            end

            def next
              return nil if @n >= 100

              batch = @collection.find(_id: { '$gte' => @n * 5000, '$lt' => (@n + 1) * 5000 }).to_a
              [ @n, batch ].tap { @n += 1 }
            end
          end

          def setup
            super
            @destination = File.join(Dir.tmpdir, 'parallel')
            FileUtils.mkdir_p(@destination)

            prepare_collection

            100.times do |n|
              insert_docs_from_file(file_name_at(n), ids_relative_to: n * 5000)
            end
          end

          def before_task
            super
            @dispatcher = Dispatcher.new(DataSource.new(collection)) do |(n, batch)|
              worker_task(n, batch)
            end
          end

          def do_task
            @dispatcher.run
          end

          def teardown
            super
            FileUtils.rm_rf(@destination)
          end

          def worker_task(index, batch)
            path = File.join(@destination, file_name_at(index))
            FileUtils.mkdir_p(File.dirname(path))
            File.write(path, batch.map(&:to_json).join("\n"))
          end
        end
      end
    end
  end
end
