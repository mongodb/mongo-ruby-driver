# frozen_string_literal: true

require_relative 'gridfs/download'
require_relative 'gridfs/upload'

module Mongo
  module DriverBench
    module Parallel
      module GridFS
        ALL = [ Download, Upload ].freeze
      end
    end
  end
end
