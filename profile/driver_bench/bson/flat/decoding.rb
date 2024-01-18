# frozen_string_literal: true

require_relative 'base'
require_relative '../decodable'

module Mongo
  module DriverBench
    module BSON
      module Flat
        class Decoding < Mongo::DriverBench::BSON::Flat::Base
          include Decodable
        end
      end
    end
  end
end
