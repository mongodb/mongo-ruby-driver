# frozen_string_literal: true

require_relative 'full/encoding'
require_relative 'full/decoding'

module Mongo
  module DriverBench
    module BSON
      module Full
        ALL = [ Encoding, Decoding ].freeze
      end
    end
  end
end
