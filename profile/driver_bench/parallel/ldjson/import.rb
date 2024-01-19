# frozen_string_literal: true

require_relative 'base'
require_relative '../dispatcher'

module Mongo
  module DriverBench
    module Parallel
      module LDJSON
        # "This benchmark tests driver performance importing documents from a
        # set of LDJSON files."
        #
        # @api private
        class Import < Mongo::DriverBench::Parallel::LDJSON::Base
          bench_name 'LDJSON multi-file import'

          private

          # The data source for this benchmark. Each batch is the name of a
          # file to read documents file.
          class DataSource
            def initialize(bench)
              @n = 0
              @bench = bench
            end

            def next
              return nil if @n >= 100

              @bench.file_name_at(@n).tap { @n += 1 }
            end
          end

          def before_task
            super
            prepare_collection
            @dispatcher = Dispatcher.new(DataSource.new(self)) do |file_name|
              insert_docs_from_file(file_name)
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
