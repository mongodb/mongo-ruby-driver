# frozen_string_literal: true

require_relative '../base'

module Mongo
  module DriverBench
    module BSON
      module Flat
        class Base < Mongo::DriverBench::BSON::Base
          private

          def file_name
            "extended_bson/flat_bson.json"
          end
        end
      end
    end
  end
end
