# frozen_string_literal: true

require_relative 'base'
require_relative '../decodable'

module Mongo
  module DriverBench
    module BSON
      module Deep
        class Decoding < Mongo::DriverBench::BSON::Deep::Base
          include Decodable
        end
      end
    end
  end
end
