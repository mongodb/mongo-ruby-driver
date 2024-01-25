# frozen_string_literal: true

require_relative '../base'

module Mongo
  module DriverBench
    module MultiDoc
      module GridFS
        # Abstract base class for multi-doc GridFS benchmarks.
        #
        # @api private
        class Base < Mongo::DriverBench::MultiDoc::Base
          private

          # how much the dataset size ought to be scaled (for scoring
          # purposes).
          def scale
            1
          end

          def file_name
            'single_and_multi_document/gridfs_large.bin'
          end

          def load_file(file_name)
            File.read(path_to_file(file_name), encoding: 'BINARY')
          end
        end
      end
    end
  end
end
