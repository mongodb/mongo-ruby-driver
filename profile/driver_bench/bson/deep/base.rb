# frozen_string_literal: true

require_relative '../base'

module Mongo
  module DriverBench
    module BSON
      module Deep
        class Base < Mongo::DriverBench::BSON::Base
          private

          def file_name
            "extended_bson/deep_bson.json"
          end
        end
      end
    end
  end
end
