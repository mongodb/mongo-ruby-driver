# frozen_string_literal: true

require_relative 'base'
require_relative '../encodable'

module Mongo
  module DriverBench
    module BSON
      module Flat
        class Encoding < Mongo::DriverBench::BSON::Flat::Base
          include Encodable
        end
      end
    end
  end
end
