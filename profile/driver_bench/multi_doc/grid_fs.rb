# frozen_string_literal: true

require_relative 'grid_fs/download'
require_relative 'grid_fs/upload'

module Mongo
  module DriverBench
    module MultiDoc
      module GridFS
        ALL = [ Download, Upload ].freeze
      end
    end
  end
end
