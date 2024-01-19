# frozen_string_literal: true

require_relative 'flat/encoding'
require_relative 'flat/decoding'

module Mongo
  module DriverBench
    module BSON
      module Flat
        ALL = [ Encoding, Decoding ].freeze
      end
    end
  end
end
