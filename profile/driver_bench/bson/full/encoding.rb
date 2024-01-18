# frozen_string_literal: true

require_relative 'base'
require_relative '../encodable'

module Mongo
  module DriverBench
    module BSON
      module Full
        class Encoding < Mongo::DriverBench::BSON::Full::Base
          include Encodable
        end
      end
    end
  end
end
