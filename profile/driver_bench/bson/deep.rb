# frozen_string_literal: true

require_relative 'deep/encoding'
require_relative 'deep/decoding'

module Mongo
  module DriverBench
    module BSON
      module Deep
        ALL = [ Encoding, Decoding ].freeze
      end
    end
  end
end
