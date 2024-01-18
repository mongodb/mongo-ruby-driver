# frozen_string_literal: true

require 'tmpdir'
require 'fileutils'

require_relative 'base'
require_relative '../dispatcher'

module Mongo
  module DriverBench
    module Parallel
      module LDJSON
        class Export < Mongo::DriverBench::Parallel::LDJSON::Base
          private

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

          def worker_task(n, batch)
            path = File.join(@destination, file_name_at(n))
            FileUtils.mkdir_p(File.dirname(path))
            File.write(path, batch.map(&:to_json).join("\n"))
          end
        end
      end
    end
  end
end
