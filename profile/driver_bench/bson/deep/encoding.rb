# frozen_string_literal: true

require_relative 'base'
require_relative '../encodable'

module Mongo
  module DriverBench
    module BSON
      module Deep
        class Encoding < Mongo::DriverBench::BSON::Deep::Base
          include Encodable
        end
      end
    end
  end
end
